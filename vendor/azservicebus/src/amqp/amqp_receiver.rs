use fe2o3_amqp::{
    link::{delivery::DeliveryInfo, DetachError, IllegalLinkStateError, RecvError},
    Delivery, Receiver,
};
use fe2o3_amqp_types::{
    definitions::{ErrorCondition, Fields, ReceiverSettleMode},
    messaging::{annotations::AnnotationKey, Body, Modified},
    primitives::{Array, OrderedMap, Symbol, Timestamp, Uuid},
};
use serde_amqp::Value;
use std::future::Future;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration as StdDuration,
};
use tokio::sync::{mpsc, Mutex};
use url::Url;

use crate::{
    core::{RecoverableTransport, TransportReceiver},
    primitives::{
        disposition_status::DispositionStatus,
        error::RetryError,
        service_bus_peeked_message::ServiceBusPeekedMessage,
        service_bus_received_message::{ReceivedMessageLockToken, ServiceBusReceivedMessage},
        service_bus_retry_policy::{run_operation, ServiceBusRetryPolicy},
    },
    sealed::Sealed,
    ServiceBusReceiveMode,
};

use super::{
    amqp_cbs_link,
    amqp_client_constants::DEAD_LETTER_NAME,
    amqp_connection_scope::AmqpConnectionScope,
    amqp_connection_scope::ReceiverType,
    amqp_management_link::AmqpManagementLink,
    amqp_message_constants::{DEAD_LETTER_ERROR_DESCRIPTION_HEADER, DEAD_LETTER_REASON_HEADER},
    amqp_message_converter::LOCK_TOKEN_DELIVERY_ANNOTATION,
    amqp_request_message::{
        peek_message::PeekMessageRequest, peek_session_message::PeekSessionMessageRequest,
        receive_by_sequence_number::ReceiveBySequenceNumberRequest, renew_lock::RenewLockRequest,
        update_disposition::UpdateDispositionRequest,
    },
    amqp_response_message::{
        peek_message::PeekMessageResponse, peek_session_message::PeekSessionMessageResponse,
        receive_by_sequence_number::ReceiveBySequenceNumberResponse, renew_lock::RenewLockResponse,
    },
    error::{AmqpDispositionError, AmqpRecvError, AmqpRequestResponseError, RecoverReceiverError},
};

pub(super) async fn receive_messages(
    receiver: &mut Receiver,
    prefetch_count: u32,
    receive_mode: &ServiceBusReceiveMode,
    receiver_epoch: uuid::Uuid,
    buffer: &mut VecDeque<ServiceBusReceivedMessage>,
    max_messages: u32,
) -> Result<(), AmqpRecvError> {
    // Credit mode is manual, need to set credit
    let remaining = max_messages.saturating_sub(buffer.len() as u32);
    if remaining == 0 {
        return Ok(());
    }
    if prefetch_count == 0 {
        receiver.set_credit(remaining).await?;
    }

    while buffer.len() < max_messages as usize {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        let lock_token =
            lock_token_from_delivery(&delivery).ok_or(AmqpRecvError::LockTokenNotFound)?;
        let (delivery_info, raw_amqp_message) = delivery.into_parts();
        let is_settled = *receive_mode == ServiceBusReceiveMode::ReceiveAndDelete;
        let accepted_info = is_settled.then(|| delivery_info.clone());
        let lock_token = ReceivedMessageLockToken::Delivery {
            receiver_epoch,
            delivery_info,
            lock_token,
        };
        let message = ServiceBusReceivedMessage {
            _is_settled: is_settled,
            raw_amqp_message,
            lock_token,
        };

        // No await may separate acquiring a delivery from retaining its custody.
        buffer.push_back(message);
        if let Some(info) = accepted_info {
            receiver.accept(info).await.map_err(RecvError::from)?;
        }
    }
    Ok(())
}

fn lock_token_from_delivery<B>(delivery: &Delivery<B>) -> Option<Uuid> {
    match delivery
        .message()
        .delivery_annotations
        .as_ref()
        .and_then(|da| da.get(&LOCK_TOKEN_DELIVERY_ANNOTATION as &dyn AnnotationKey))
    {
        Some(Value::Uuid(uuid)) => Some(uuid.clone()),
        _ => {
            let delivery_tag = delivery.delivery_tag().as_ref();
            match Uuid::try_from(delivery_tag) {
                Ok(uuid) => Some(uuid),
                _ => None,
            }
        }
    }
}

// TODO: reduce clone
fn map_properties_to_modify_into_fields(
    properties_to_modify: &OrderedMap<String, Value>,
) -> Fields {
    properties_to_modify
        .iter()
        .map(|(k, v)| (Symbol::from(k.as_str()), v.clone()))
        .collect()
}

// The management response has already confirmed the business operation. Advancing
// the link's local disposition/credit must neither replay that operation nor turn
// its known success into failure if this best-effort cleanup cannot finish.
async fn finish_confirmed_disposition(
    try_timeout: StdDuration,
    operation: impl Future<Output = Result<(), AmqpDispositionError>>,
) {
    match crate::util::time::timeout(try_timeout, operation).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!(
            "Confirmed disposition; local link cleanup failed: {}",
            error
        ),
        Err(_) => log::warn!("Confirmed disposition; local link cleanup timed out"),
    }
}

// Some brokers acknowledge a stale abandon/dead-letter management request as a
// no-op. A successful status cannot confirm custody we already know has expired.
// The latest broker-supplied deadline (updated by renewal) is checked on every
// attempt, before the request future is polled. Missing deadlines, including
// session deliveries, still require the broker's management response.
async fn confirm_live_disposition<T>(
    locked_until: Option<time::OffsetDateTime>,
    mut now: impl FnMut() -> time::OffsetDateTime,
    operation: impl Future<Output = Result<T, fe2o3_amqp_management::error::Error>>,
) -> Result<T, AmqpDispositionError> {
    if locked_until.is_some_and(|until| until <= now()) {
        return Err(AmqpDispositionError::MessageLockExpired);
    }
    let response = operation.await.map_err(AmqpDispositionError::from)?;
    // If the deadline passed in flight, a successful no-op response is still
    // possible. Preserve uncertainty instead of claiming confirmed settlement.
    if locked_until.is_some_and(|until| until <= now()) {
        return Err(AmqpDispositionError::MessageLockExpired);
    }
    Ok(response)
}

fn disposition_lock_deadline(
    message: &ServiceBusReceivedMessage,
    session_id: Option<&str>,
    link_properties: &Option<Fields>,
) -> Option<time::OffsetDateTime> {
    if session_id.is_some() {
        // Session messages may carry an unbounded per-message sentinel. The
        // receiver's session lease is authoritative, including after renewal.
        super::amqp_session_receiver::get_session_locked_until(link_properties)
    } else {
        message.locked_until()
    }
}

/// An AMQP receiver implementation for Service Bus
#[derive(Debug)]
pub struct AmqpReceiver {
    pub(crate) id: u32, // TODO: should this info be preserved?
    pub(crate) service_endpoint: Arc<Url>,
    pub(crate) entity_path: String,
    pub(crate) identifier_str: String,

    pub(crate) prefetch_count: u32,
    pub(crate) retry_policy: Box<dyn ServiceBusRetryPolicy>,
    pub(crate) receiver: Option<fe2o3_amqp::Receiver>,
    pub(crate) receiver_type: ReceiverType,
    pub(crate) link_name: String,
    pub(super) receive_recovery: super::receive_batch::Recovery,
    pub(crate) receiver_epoch: uuid::Uuid,
    pub(crate) receive_mode: ServiceBusReceiveMode,
    pub(crate) _is_processor: bool, // TODO: implement processor

    pub(crate) management_link: AmqpManagementLink,
    pub(crate) request_response_locked_messages: HashSet<fe2o3_amqp::types::primitives::Uuid>,
    pub(crate) last_peeked_sequence_number: i64,

    pub(crate) cbs_command_sender: mpsc::Sender<amqp_cbs_link::Command>,

    /// This is ONLY used for recovery
    pub(crate) connection_scope: Arc<Mutex<AmqpConnectionScope>>,
}

impl RecoverableTransport for AmqpReceiver {
    type RecoverError = RecoverReceiverError;

    async fn recover(&mut self) -> Result<(), Self::RecoverError> {
        self.finish_credit_cleanup().await;
        if self.receive_recovery.operation.is_none() {
            self.receive_recovery.needed = true;
            crate::util::time::sleep(self.receive_recovery.remaining()).await;
            self.cache_session_lease();
            // Rotate only when a recovery actually starts, not on every short poll.
            self.receiver_epoch = uuid::Uuid::new_v4();
            self.request_response_locked_messages.clear();
            let mut transport = super::receiver_recovery::ReceiverRecovery {
                id: self.id,
                receiver: self.receiver.take(),
                management_link: None,
                cbs_command_sender: self.cbs_command_sender.clone(),
                service_endpoint: self.service_endpoint.clone(),
                entity_path: self.entity_path.clone(),
                identifier: self.identifier_str.clone(),
                receive_mode: self.receive_mode,
                receiver_type: self.receiver_type.clone(),
                prefetch_count: self.prefetch_count,
                connection_scope: self.connection_scope.clone(),
            };
            self.receive_recovery.operation =
                Some(super::receive_batch::OwnedOperation::new(async move {
                    let result = transport.run().await;
                    (transport, result)
                }));
        }
        let Some(operation) = self.receive_recovery.operation.as_mut() else {
            return Err(RecoverReceiverError::ConnectionScopeDisposed);
        };
        // Await a reference: caller cancellation leaves the owned future intact.
        let (transport, result) = operation.await;
        self.receive_recovery.operation = None;
        self.id = transport.id;
        self.receiver = transport.receiver;
        self.cbs_command_sender = transport.cbs_command_sender;
        if let Some(receiver) = &self.receiver {
            self.link_name = receiver.name().to_owned();
        }
        if let Some(management) = transport.management_link {
            self.management_link = management;
        }
        self.cache_session_lease();
        match &result {
            Ok(()) => self.receive_recovery.recovered(),
            Err(error) => {
                self.receive_recovery.failed(&*self.retry_policy, error);
            }
        }
        result
    }
}

impl AmqpReceiver {
    fn cache_session_lease(&mut self) {
        if let Some(receiver) = &self.receiver {
            self.receive_recovery.session_locked_until =
                receiver.properties(super::amqp_session_receiver::get_session_locked_until);
        }
    }

    pub(super) fn session_id(&self) -> Option<&str> {
        match &self.receiver_type {
            ReceiverType::Session { session_id } => session_id.as_deref(),
            ReceiverType::NonSession => None,
        }
    }

    pub(super) fn session_locked_until(&self) -> time::OffsetDateTime {
        match self.receiver.as_ref() {
            Some(receiver) => {
                receiver.properties(super::amqp_session_receiver::get_session_locked_until)
            }
            None => self.receive_recovery.session_locked_until,
        }
        .unwrap_or(crate::constants::DEFAULT_OFFSET_DATE_TIME)
    }

    pub(super) fn set_session_locked_until(&mut self, until: time::OffsetDateTime) {
        self.receive_recovery.session_locked_until = Some(until);
        if let Some(receiver) = &mut self.receiver {
            receiver.properties_mut(|properties| {
                properties.get_or_insert_with(Fields::default).insert(
                    super::amqp_client_constants::LOCKED_UNTIL_UTC.into(),
                    Value::Timestamp(Timestamp::from(until)),
                );
            });
        }
    }

    pub(super) async fn finish_credit_cleanup(&mut self) {
        if let Some(operation) = self.receive_recovery.cleanup.as_mut() {
            let (receiver, result) = operation.await;
            self.receive_recovery.cleanup = None;
            self.receiver = Some(receiver);
            self.receive_recovery.cleanup_error = result.err();
        }
        if let Some(error) = self.receive_recovery.cleanup_error.take() {
            self.receive_recovery
                .failed(&*self.retry_policy, &AmqpRecvError::from(error));
        }
    }

    /// Management must never race an unfinished recovery or use its old link.
    pub(super) async fn prepare_management(&mut self) -> Result<(), AmqpRequestResponseError> {
        self.finish_credit_cleanup().await;
        if self.receive_recovery.needed {
            self.recover().await.map_err(|_| {
                AmqpRequestResponseError::RequestResponse(
                    fe2o3_amqp_management::error::Error::Send(
                        fe2o3_amqp::link::SendError::LinkStateError(
                            fe2o3_amqp::link::LinkStateError::IllegalSessionState,
                        ),
                    ),
                )
            })?;
        }
        Ok(())
    }

    fn receiver_mut(&mut self) -> Result<&mut Receiver, IllegalLinkStateError> {
        self.receiver
            .as_mut()
            .ok_or(IllegalLinkStateError::IllegalState)
    }

    async fn receive_messages_with_timeout(
        &mut self,
        prefetch_count: u32,
        max_messages: u32,
    ) -> Result<(), AmqpRecvError> {
        self.cache_session_lease();
        let lease = super::receive_batch::CreditLease {
            slot: &mut self.receiver,
            cleanup: &mut self.receive_recovery.cleanup,
            cleanup_error: &mut self.receive_recovery.cleanup_error,
            manual: prefetch_count == 0,
        };
        let receiver = lease
            .slot
            .as_mut()
            .ok_or(IllegalLinkStateError::IllegalState)?;
        receive_messages(
            receiver,
            prefetch_count,
            &self.receive_mode,
            self.receiver_epoch,
            &mut self.receive_recovery.buffer,
            max_messages,
        )
        .await
    }

    async fn complete_message(
        &mut self,
        delivery_info: &DeliveryInfo,
    ) -> Result<(), AmqpDispositionError> {
        // TODO: avoid clone
        self.receiver_mut()?.accept(delivery_info.clone()).await?;
        Ok(())
    }

    async fn dead_letter_message(
        &mut self,
        delivery_info: &DeliveryInfo,
        dead_letter_reason: &Option<String>,
        dead_letter_error_description: &Option<String>,
        properties_to_modify: &Option<OrderedMap<String, Value>>,
    ) -> Result<(), AmqpDispositionError> {
        let mut error = None;
        if dead_letter_reason.is_some()
            || dead_letter_error_description.is_some()
            || properties_to_modify.is_some()
        {
            let condition = ErrorCondition::Custom(Symbol::from(DEAD_LETTER_NAME));
            let mut info = None;

            if let Some(reason) = dead_letter_reason {
                info.get_or_insert(Fields::default()).insert(
                    DEAD_LETTER_REASON_HEADER.into(),
                    Value::from(reason.as_str()),
                );
            }

            if let Some(description) = dead_letter_error_description {
                info.get_or_insert(Fields::default()).insert(
                    DEAD_LETTER_ERROR_DESCRIPTION_HEADER.into(),
                    Value::from(description.as_str()),
                );
            }

            if let Some(properties_to_modify) = properties_to_modify {
                for (k, v) in properties_to_modify {
                    info.get_or_insert(Fields::default())
                        .insert(Symbol::from(k.as_str()), v.clone()); // TODO: reduce cloning
                }
            }

            error = Some(fe2o3_amqp::types::definitions::Error::new(
                condition, None, info,
            ))
        }

        // TODO: avoid clone
        self.receiver_mut()?
            .reject(delivery_info.clone(), error)
            .await?;
        Ok(())
    }

    async fn abandon_message(
        &mut self,
        delivery_info: &DeliveryInfo,
        properties_to_modify: &Option<OrderedMap<String, Value>>,
    ) -> Result<(), AmqpDispositionError> {
        let modified = Modified {
            delivery_failed: None,
            undeliverable_here: None,
            message_annotations: properties_to_modify
                .as_ref()
                .map(map_properties_to_modify_into_fields),
        };

        self.receiver_mut()?
            .modify(delivery_info.clone(), modified)
            .await?;
        Ok(())
    }

    async fn defer_message(
        &mut self,
        delivery_info: &DeliveryInfo,
        properties_to_modify: &Option<OrderedMap<String, Value>>,
    ) -> Result<(), AmqpDispositionError> {
        let modified = Modified {
            delivery_failed: None,
            undeliverable_here: Some(true),
            message_annotations: properties_to_modify
                .as_ref()
                .map(map_properties_to_modify_into_fields),
        };

        self.receiver_mut()?
            .modify(delivery_info.clone(), modified)
            .await?;
        Ok(())
    }

    async fn receive_by_sequence_number(
        &mut self,
        request: &mut ReceiveBySequenceNumberRequest,
        try_timeout: &StdDuration,
    ) -> Result<ReceiveBySequenceNumberResponse, AmqpRequestResponseError> {
        self.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.management_link.call(request).await?;
        Ok(response)
    }

    async fn update_disposition(
        &mut self,
        message: &ServiceBusReceivedMessage,
        session_id: Option<&str>,
        request: &mut UpdateDispositionRequest,
        try_timeout: &StdDuration,
    ) -> Result<(), AmqpDispositionError> {
        self.finish_credit_cleanup().await;
        match &message.lock_token {
            ReceivedMessageLockToken::Delivery { receiver_epoch, .. }
                if *receiver_epoch != self.receiver_epoch
                    || self.receiver.is_none()
                    || self.receive_recovery.needed =>
            {
                return Err(AmqpDispositionError::MessageLockLost)
            }
            ReceivedMessageLockToken::LockToken(token)
                if !self.request_response_locked_messages.contains(token) =>
            {
                return Err(AmqpDispositionError::MessageLockLost)
            }
            _ => {}
        }
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));
        let locked_until = self
            .receiver
            .as_ref()
            .map(|receiver| {
                receiver.properties(|properties| {
                    disposition_lock_deadline(message, session_id, properties)
                })
            })
            .unwrap_or_else(|| disposition_lock_deadline(message, session_id, &None));
        let _response = confirm_live_disposition(
            locked_until,
            crate::util::time::now_utc,
            self.management_link.call(request),
        )
        .await?;
        Ok(())
    }

    async fn peek_message_inner(
        &mut self,
        request: &mut PeekMessageRequest,
        try_timeout: &StdDuration,
    ) -> Result<PeekMessageResponse, AmqpRequestResponseError> {
        self.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.management_link.call(request).await?;
        Ok(response)
    }

    async fn peek_session_message_inner(
        &mut self,
        request: &mut PeekSessionMessageRequest,
        try_timeout: &StdDuration,
    ) -> Result<PeekSessionMessageResponse, AmqpRequestResponseError> {
        self.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.management_link.call(request).await?;
        Ok(response)
    }

    async fn renew_lock(
        &mut self,
        request: &mut RenewLockRequest,
        try_timeout: &StdDuration,
    ) -> Result<RenewLockResponse, AmqpRequestResponseError> {
        self.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.management_link.call(request).await?;
        Ok(response)
    }
}

impl Sealed for AmqpReceiver {}

impl TransportReceiver for AmqpReceiver {
    type RequestResponseError = RetryError<AmqpRequestResponseError>;
    type ReceiveError = RetryError<AmqpRecvError>;
    type DispositionError = RetryError<AmqpDispositionError>;
    type CloseError = DetachError;

    fn entity_path(&self) -> &str {
        &self.entity_path
    }

    fn identifier(&self) -> &str {
        &self.identifier_str
    }

    fn prefetch_count(&self) -> u32 {
        self.prefetch_count
    }

    fn receive_mode(&self) -> ServiceBusReceiveMode {
        self.receive_mode
    }

    async fn receive_messages(
        &mut self,
        max_messages: u32,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::ReceiveError> {
        let default_max_wait_time = self.retry_policy.options().try_timeout;
        loop {
            let buffer = self
                .receive_messages_with_max_wait_time(max_messages, Some(default_max_wait_time))
                .await?;
            if !buffer.is_empty() {
                return Ok(buffer);
            }
        }
    }

    /// Receives a set of [`ServiceBusReceivedMessage`] from the entity using [`ServiceBusReceiveMode`] mode.
    async fn receive_messages_with_max_wait_time(
        &mut self,
        max_messages: u32,
        max_wait_time: Option<StdDuration>,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::ReceiveError> {
        let max_wait_time =
            max_wait_time.unwrap_or_else(|| self.retry_policy.options().try_timeout);
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        // A caller may have dropped its receive after one or more transfers.
        // Return that custody before attempting any link recovery or new credit.
        if !self.receive_recovery.buffer.is_empty() {
            return Ok(self.receive_recovery.take_batch(max_messages));
        }
        let result = crate::util::time::timeout(max_wait_time, async {
            loop {
                self.finish_credit_cleanup().await;
                if self.receive_recovery.needed {
                    self.recover()
                        .await
                        .map_err(|_| RetryError::Operation(AmqpRecvError::RecoveryPending))?;
                }
                let attempt = self
                    .receive_messages_with_timeout(self.prefetch_count, max_messages)
                    .await;
                match attempt {
                    Ok(()) => {
                        self.receive_recovery.healthy();
                        return Ok(());
                    }
                    Err(error) => {
                        let retry = self.receive_recovery.failed(&*self.retry_policy, &error);
                        if !self.receive_recovery.buffer.is_empty() {
                            return Ok(());
                        }
                        if !retry {
                            return Err(RetryError::Operation(error));
                        }
                    }
                }
            }
        })
        .await;
        // Cleanup can fail during CreditLease::drop at the deadline; record that
        // failure synchronously before deciding whether this was a healthy idle.
        if let Some(error) = self.receive_recovery.cleanup_error.take() {
            self.receive_recovery
                .failed(&*self.retry_policy, &AmqpRecvError::from(error));
        }
        if result.is_err() && !self.receive_recovery.pending() {
            // Reaching the receive deadline on a healthy link is ordinary idle,
            // not another failure in the transport's persistent retry sequence.
            self.receive_recovery.healthy();
        }
        if !self.receive_recovery.buffer.is_empty() {
            return Ok(self.receive_recovery.take_batch(max_messages));
        }
        match result {
            Ok(Err(error)) => Err(error),
            _ if self.receive_recovery.pending() => {
                Err(RetryError::Operation(AmqpRecvError::RecoveryPending))
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Closes the connection to the transport consumer instance.
    async fn close(mut self) -> Result<(), Self::CloseError> {
        self.finish_credit_cleanup().await;
        if self.receive_recovery.operation.is_some() {
            // Finish the owned handshake before closing its resources. Cancelling
            // a close drops the whole receiver, not a supposedly reusable link.
            let _ = self.recover().await;
        }
        let _ = self
            .cbs_command_sender
            .send(amqp_cbs_link::Command::RemoveAuthorizationRefresher(
                self.id,
            ))
            .await;
        if let Some(mut receiver) = self.receiver.take() {
            receiver.set_credit(0).await?;
            receiver.close().await?;
        }
        self.management_link.close().await?;
        Ok(())
    }

    /// Completes a [`ServiceBusReceivedMessage`]. This will delete the message from the service.
    async fn complete(
        &mut self,
        message: &ServiceBusReceivedMessage,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        let policy = &mut self.retry_policy;
        let mut try_timeout = policy.calculate_try_timeout(0);

        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(lock_token)
            | ReceivedMessageLockToken::Delivery { lock_token, .. } => {
                let mut request = UpdateDispositionRequest::new(
                    DispositionStatus::Completed,
                    Array(vec![lock_token.clone()]), // TODO: reduce clone
                    None,
                    None,
                    None,
                    session_id,
                    Some(self.link_name.clone()),
                );
                run_operation!(
                    { &self.retry_policy },
                    AmqpDispositionError,
                    try_timeout,
                    self.update_disposition(message, session_id, &mut request, &try_timeout),
                    self.recover()
                )?;

                self.request_response_locked_messages.remove(lock_token);
                if let ReceivedMessageLockToken::Delivery { delivery_info, .. } =
                    &message.lock_token
                {
                    // Management success is the confirmation. This link disposition only
                    // advances local bookkeeping/credit; it must not trigger another retry
                    // of the already-confirmed operation if the link has since failed.
                    finish_confirmed_disposition(try_timeout, self.complete_message(delivery_info))
                        .await;
                }
            }
        };

        Ok(())
    }

    /// Indicates that the receiver wants to defer the processing for the message.
    async fn defer(
        &mut self,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        let policy = &mut self.retry_policy;
        let mut try_timeout = policy.calculate_try_timeout(0);

        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(lock_token)
            | ReceivedMessageLockToken::Delivery { lock_token, .. } => {
                let mut request = UpdateDispositionRequest::new(
                    DispositionStatus::Defered,
                    Array(vec![lock_token.clone()]), // TODO: reduce clone
                    None,
                    None,
                    properties_to_modify.clone(),
                    session_id,
                    Some(self.link_name.clone()),
                );
                run_operation!(
                    { &self.retry_policy },
                    AmqpDispositionError,
                    try_timeout,
                    self.update_disposition(message, session_id, &mut request, &try_timeout),
                    self.recover()
                )?;

                self.request_response_locked_messages.remove(lock_token);
                if let ReceivedMessageLockToken::Delivery { delivery_info, .. } =
                    &message.lock_token
                {
                    finish_confirmed_disposition(
                        try_timeout,
                        self.defer_message(delivery_info, &properties_to_modify),
                    )
                    .await;
                }
            }
        };
        Ok(())
    }

    /// Fetches the next batch of active messages without changing the state of the receiver or the message source.
    async fn peek_messages(
        &mut self,
        sequence_number: Option<i64>,
        message_count: i32,
    ) -> Result<Vec<ServiceBusPeekedMessage>, Self::RequestResponseError> {
        let mut request = PeekMessageRequest::new(
            sequence_number.unwrap_or(self.last_peeked_sequence_number + 1),
            message_count,
            Some(self.link_name.clone()),
        );
        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.peek_message_inner(&mut request, &try_timeout),
            self.recover()
        )?;

        let peeked_messages = response
            .into_peeked_messages()
            .map_err(AmqpRequestResponseError::from)
            .map_err(RetryError::Operation)?;

        if let Some(last) = peeked_messages.last() {
            self.last_peeked_sequence_number = last.sequence_number();
        }
        Ok(peeked_messages)
    }

    async fn peek_session_messages(
        &mut self,
        sequence_number: Option<i64>,
        message_count: i32,
        session_id: &str,
    ) -> Result<Vec<ServiceBusPeekedMessage>, Self::RequestResponseError> {
        let mut request = PeekSessionMessageRequest::new(
            sequence_number.unwrap_or(self.last_peeked_sequence_number + 1),
            message_count,
            session_id,
            Some(self.link_name.clone()),
        );
        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.peek_session_message_inner(&mut request, &try_timeout),
            self.recover()
        )?;

        let peeked_messages = response
            .into_peeked_messages()
            .map_err(AmqpRequestResponseError::from)
            .map_err(RetryError::Operation)?;

        if let Some(last) = peeked_messages.last() {
            self.last_peeked_sequence_number = last.sequence_number();
        }
        Ok(peeked_messages)
    }

    /// Abandons a [`ServiceBusReceivedMessage`]. This will make the message available again for processing.
    async fn abandon(
        &mut self,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(lock_token)
            | ReceivedMessageLockToken::Delivery { lock_token, .. } => {
                let mut request = UpdateDispositionRequest::new(
                    DispositionStatus::Abandoned,
                    Array(vec![lock_token.clone()]), // TODO: reduce clone
                    None,
                    None,
                    properties_to_modify.clone(),
                    session_id,
                    Some(self.link_name.clone()),
                );
                run_operation!(
                    { &self.retry_policy },
                    AmqpDispositionError,
                    try_timeout,
                    self.update_disposition(message, session_id, &mut request, &try_timeout),
                    self.recover()
                )?;

                self.request_response_locked_messages.remove(lock_token);
                if let ReceivedMessageLockToken::Delivery { delivery_info, .. } =
                    &message.lock_token
                {
                    finish_confirmed_disposition(
                        try_timeout,
                        self.abandon_message(delivery_info, &properties_to_modify),
                    )
                    .await;
                }
            }
        };
        Ok(())
    }

    /// Moves a message to the dead-letter subqueue.
    async fn dead_letter(
        &mut self,
        message: &ServiceBusReceivedMessage,
        dead_letter_reason: Option<String>,
        dead_letter_error_description: Option<String>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(lock_token)
            | ReceivedMessageLockToken::Delivery { lock_token, .. } => {
                let mut request = UpdateDispositionRequest::new(
                    DispositionStatus::Suspended,
                    Array(vec![lock_token.clone()]),
                    dead_letter_reason.clone(),
                    dead_letter_error_description.clone(),
                    properties_to_modify.clone(),
                    session_id,
                    Some(self.link_name.clone()),
                );
                run_operation!(
                    { &self.retry_policy },
                    AmqpDispositionError,
                    try_timeout,
                    self.update_disposition(message, session_id, &mut request, &try_timeout),
                    self.recover()
                )?;

                self.request_response_locked_messages.remove(lock_token);
                if let ReceivedMessageLockToken::Delivery { delivery_info, .. } =
                    &message.lock_token
                {
                    finish_confirmed_disposition(
                        try_timeout,
                        self.dead_letter_message(
                            delivery_info,
                            &dead_letter_reason,
                            &dead_letter_error_description,
                            &properties_to_modify,
                        ),
                    )
                    .await;
                }
            }
        }

        Ok(())
    }

    /// Receives a [`Vec<ServiceBusReceivedMessages>`] of deferred messages identified by `sequence_numbers`.
    async fn receive_deferred_messages(
        &mut self,
        sequence_numbers: impl Iterator<Item = i64> + Send,
        session_id: Option<&str>,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::RequestResponseError> {
        let sequence_numbers = sequence_numbers.collect();
        let receiver_settle_mode = match self.receive_mode {
            ServiceBusReceiveMode::PeekLock => ReceiverSettleMode::Second,
            ServiceBusReceiveMode::ReceiveAndDelete => ReceiverSettleMode::First,
        };
        let mut request = ReceiveBySequenceNumberRequest::new(
            sequence_numbers,
            receiver_settle_mode,
            session_id,
            Some(self.link_name.clone()),
        );

        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.receive_by_sequence_number(&mut request, &try_timeout),
            self.recover()
        )?;

        let received_messages = response
            .into_received_messages()
            .map_err(AmqpRequestResponseError::from)
            .map_err(RetryError::Operation)?;

        for message in &received_messages {
            if let ReceivedMessageLockToken::LockToken(lock_token) = &message.lock_token {
                self.request_response_locked_messages
                    .insert(lock_token.clone());
            }
        }

        Ok(received_messages)
    }

    /// Renews the lock on the message. The lock will be renewed based on the setting specified on the queue.
    async fn renew_message_lock(
        &mut self,
        lock_tokens: Vec<Uuid>,
    ) -> Result<Vec<Timestamp>, Self::RequestResponseError> {
        let mut request = RenewLockRequest::new(Array(lock_tokens), Some(self.link_name.clone()));
        let mut try_timeout = self.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.renew_lock(&mut request, &try_timeout),
            self.recover()
        )?;
        Ok(response.expirations.into_inner())
    }
}

#[cfg(test)]
mod disposition_tests {
    use std::cell::Cell;

    use fe2o3_amqp_management::error::Error as ManagementError;
    use fe2o3_amqp_types::{
        definitions::Fields,
        messaging::{Body, Message},
        primitives::{Timestamp, Uuid},
    };
    use serde_amqp::Value;
    use time::{Duration, OffsetDateTime};

    use super::{confirm_live_disposition, disposition_lock_deadline, AmqpDispositionError};
    use crate::amqp::amqp_client_constants::LOCKED_UNTIL_UTC;
    use crate::primitives::service_bus_received_message::{
        ReceivedMessageLockToken, ServiceBusReceivedMessage,
    };

    fn message(locked_until: Option<OffsetDateTime>) -> ServiceBusReceivedMessage {
        let mut message = ServiceBusReceivedMessage {
            _is_settled: false,
            raw_amqp_message: Message::builder().body(Body::<Value>::Empty).build(),
            lock_token: ReceivedMessageLockToken::LockToken(Uuid::from([1; 16])),
        };
        if let Some(deadline) = locked_until {
            message.set_locked_until(deadline);
        }
        message
    }

    #[tokio::test]
    async fn known_expired_locks_cannot_succeed_even_if_broker_would_acknowledge_a_no_op() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        for deadline in [now - Duration::milliseconds(1), now] {
            let message = message(Some(deadline));
            let sent = Cell::new(false);
            let result = confirm_live_disposition(message.locked_until(), || now, async {
                sent.set(true);
                Ok(())
            })
            .await;
            assert!(matches!(
                result,
                Err(AmqpDispositionError::MessageLockExpired)
            ));
            assert!(
                !sent.get(),
                "an already-expired disposition reached the broker"
            );
            assert!(!message._is_settled);
        }
    }

    #[tokio::test]
    async fn live_or_unknown_deadlines_still_require_broker_confirmation() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        for deadline in [Some(now + Duration::minutes(1)), None] {
            let message = message(deadline);
            let result = confirm_live_disposition(message.locked_until(), || now, async {
                Err::<(), _>(ManagementError::StatusCodeNotFound)
            })
            .await;
            assert!(matches!(
                result,
                Err(AmqpDispositionError::RequestResponse(_))
            ));
            assert!(
                confirm_live_disposition(message.locked_until(), || now, async { Ok(()) })
                    .await
                    .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn renewal_and_retries_use_the_latest_deadline_on_each_attempt() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut message = message(Some(now));
        assert!(matches!(
            confirm_live_disposition(message.locked_until(), || now, async { Ok(()) }).await,
            Err(AmqpDispositionError::MessageLockExpired)
        ));
        let renewed = now + Duration::minutes(1);
        message.set_locked_until(renewed);
        assert!(
            confirm_live_disposition(message.locked_until(), || now, async { Ok(()) })
                .await
                .is_ok()
        );
        let sent = Cell::new(false);
        let retried = confirm_live_disposition(message.locked_until(), || renewed, async {
            sent.set(true);
            Ok(())
        })
        .await;
        assert!(matches!(
            retried,
            Err(AmqpDispositionError::MessageLockExpired)
        ));
        assert!(!sent.get());
    }

    #[tokio::test]
    async fn successful_response_after_expiry_remains_uncertain() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let deadline = now + Duration::seconds(1);
        let message = message(Some(deadline));
        let clock = Cell::new(now);
        let result = confirm_live_disposition(message.locked_until(), || clock.get(), async {
            clock.set(deadline);
            Ok(())
        })
        .await;
        assert!(matches!(
            result,
            Err(AmqpDispositionError::MessageLockExpired)
        ));
        assert!(!message._is_settled);
    }

    #[tokio::test]
    async fn session_settlement_uses_the_receiver_lease_not_message_annotations() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let past = now - Duration::seconds(1);
        let future = now + Duration::minutes(1);
        let mut message = message(Some(past));
        let mut fields = Fields::default();
        fields.insert(
            LOCKED_UNTIL_UTC.into(),
            Value::Timestamp(Timestamp::from(future)),
        );
        let mut properties = Some(fields);
        let deadline = disposition_lock_deadline(&message, Some("session"), &properties);
        assert_eq!(deadline, Some(future));
        assert!(confirm_live_disposition(deadline, || now, async { Ok(()) })
            .await
            .is_ok());
        assert_eq!(
            disposition_lock_deadline(&message, None, &properties),
            Some(past)
        );

        // The Microsoft emulator emits this unbounded message-lock sentinel for
        // session deliveries. It must not override the actual session deadline.
        message.set_locked_until(Timestamp::from_milliseconds(i64::MAX));
        assert_eq!(message.locked_until(), None);
        let deadline = disposition_lock_deadline(&message, Some("session"), &properties);
        assert_eq!(deadline, Some(future));
        assert!(confirm_live_disposition(deadline, || now, async { Ok(()) })
            .await
            .is_ok());
        properties.as_mut().unwrap().insert(
            LOCKED_UNTIL_UTC.into(),
            Value::Timestamp(Timestamp::from(past)),
        );
        let deadline = disposition_lock_deadline(&message, Some("session"), &properties);
        assert_eq!(deadline, Some(past));
        assert!(matches!(
            confirm_live_disposition(deadline, || now, async { Ok(()) }).await,
            Err(AmqpDispositionError::MessageLockExpired)
        ));
    }

    #[test]
    fn absent_or_invalid_session_deadlines_do_not_fall_back_to_message_expiry() {
        let message = message(Some(OffsetDateTime::UNIX_EPOCH));
        assert_eq!(
            disposition_lock_deadline(&message, Some("session"), &None),
            None
        );
        for value in [
            Value::Null,
            Value::String("invalid".into()),
            Value::Timestamp(Timestamp::from_milliseconds(i64::MIN)),
            Value::Timestamp(Timestamp::from_milliseconds(i64::MAX)),
        ] {
            let mut fields = Fields::default();
            fields.insert(LOCKED_UNTIL_UTC.into(), value);
            assert_eq!(
                disposition_lock_deadline(&message, Some("session"), &Some(fields)),
                None
            );
        }
    }
}
