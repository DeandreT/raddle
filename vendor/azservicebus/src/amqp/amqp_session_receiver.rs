use fe2o3_amqp_types::{
    definitions::Fields,
    primitives::{Binary, OrderedMap, Timestamp},
};
use serde_amqp::Value;
use std::time::Duration as StdDuration;
use time::OffsetDateTime;

use crate::{
    core::{RecoverableTransport, TransportReceiver, TransportSessionReceiver},
    primitives::{
        service_bus_peeked_message::ServiceBusPeekedMessage,
        service_bus_received_message::{lock_deadline, ServiceBusReceivedMessage},
        service_bus_retry_policy::run_operation,
    },
    sealed::Sealed,
    ServiceBusReceiveMode,
};

use super::{
    amqp_client_constants::{self, LOCKED_UNTIL_UTC},
    amqp_receiver::AmqpReceiver,
    amqp_request_message::{
        get_session_state::GetSessionStateRequest, renew_session_lock::RenewSessionLockRequest,
        set_session_state::SetSessionStateRequest,
    },
    amqp_response_message::{
        get_session_state::GetSessionStateResponse, renew_session_lock::RenewSessionLockResponse,
        set_session_state::SetSessionStateResponse,
    },
    error::{AmqpRequestResponseError, RecoverReceiverError},
};

pub(super) fn get_session_locked_until(properties: &Option<Fields>) -> Option<OffsetDateTime> {
    // Azure's long form is DateTime.Ticks: 100ns intervals since 0001-01-01,
    // not Unix milliseconds. Other peers encode the AMQP timestamp form.
    properties
        .as_ref()
        .and_then(|map| map.get(LOCKED_UNTIL_UTC))
        .and_then(|value| match value {
            Value::Long(ticks) => session_deadline_from_ticks(*ticks),
            _ => lock_deadline(value),
        })
}

fn session_deadline_from_ticks(ticks: i64) -> Option<OffsetDateTime> {
    const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
    const MAX_DATETIME_TICKS: i64 = 3_155_378_975_999_999_999;

    if !(0..=MAX_DATETIME_TICKS).contains(&ticks) {
        return None;
    }
    // Widen before subtraction/multiplication; no broker-provided i64 may panic
    // or wrap, and out-of-range sentinels must not become an expired lease.
    let unix_nanos = (i128::from(ticks) - i128::from(UNIX_EPOCH_TICKS)) * 100;
    OffsetDateTime::from_unix_timestamp_nanos(unix_nanos).ok()
}

pub(super) fn receiver_session_id(receiver: &fe2o3_amqp::Receiver) -> Option<&str> {
    receiver
        .source()
        .as_ref()
        .and_then(|source| source.filter.as_ref())
        .and_then(|filter| filter.get(amqp_client_constants::SESSION_FILTER_NAME))
        .and_then(|value| match value {
            Value::String(string) => Some(string.as_str()),
            Value::Described(described) => match &described.value {
                Value::String(string) => Some(string.as_str()),
                _ => None,
            },
            _ => None,
        })
}

/// An AMQP receiver for session enabled entities.
#[derive(Debug)]
pub struct AmqpSessionReceiver {
    pub(crate) inner: AmqpReceiver,
}

impl RecoverableTransport for AmqpSessionReceiver {
    type RecoverError = RecoverReceiverError;

    async fn recover(&mut self) -> Result<(), Self::RecoverError> {
        self.inner.recover().await
    }
}

impl AmqpSessionReceiver {
    async fn renew_session_lock_inner(
        &mut self,
        request: &mut RenewSessionLockRequest,
        try_timeout: &StdDuration,
    ) -> Result<RenewSessionLockResponse, AmqpRequestResponseError> {
        self.inner.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.inner.management_link.call(request).await?;
        Ok(response)
    }

    async fn set_session_state_inner(
        &mut self,
        request: &mut SetSessionStateRequest,
        try_timeout: &StdDuration,
    ) -> Result<SetSessionStateResponse, AmqpRequestResponseError> {
        self.inner.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.inner.management_link.call(request).await?;
        Ok(response)
    }

    async fn get_session_state_inner(
        &mut self,
        request: &mut GetSessionStateRequest,
        try_timeout: &StdDuration,
    ) -> Result<GetSessionStateResponse, AmqpRequestResponseError> {
        self.inner.prepare_management().await?;
        let server_timeout = try_timeout.as_millis() as u32;
        request.set_server_timeout(Some(server_timeout));

        let response = self.inner.management_link.call(request).await?;
        Ok(response)
    }
}

impl Sealed for AmqpSessionReceiver {}

impl TransportReceiver for AmqpSessionReceiver {
    type RequestResponseError = <AmqpReceiver as TransportReceiver>::RequestResponseError;
    type ReceiveError = <AmqpReceiver as TransportReceiver>::ReceiveError;
    type DispositionError = <AmqpReceiver as TransportReceiver>::DispositionError;
    type CloseError = <AmqpReceiver as TransportReceiver>::CloseError;

    fn entity_path(&self) -> &str {
        self.inner.entity_path()
    }

    fn identifier(&self) -> &str {
        self.inner.identifier()
    }

    fn prefetch_count(&self) -> u32 {
        self.inner.prefetch_count()
    }

    fn receive_mode(&self) -> ServiceBusReceiveMode {
        self.inner.receive_mode()
    }

    async fn receive_messages(
        &mut self,
        max_messages: u32,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::ReceiveError> {
        self.inner.receive_messages(max_messages).await
    }

    async fn receive_messages_with_max_wait_time(
        &mut self,
        max_messages: u32,
        max_wait_time: Option<StdDuration>,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::ReceiveError> {
        self.inner
            .receive_messages_with_max_wait_time(max_messages, max_wait_time)
            .await
    }

    async fn close(self) -> Result<(), Self::CloseError> {
        self.inner.close().await
    }

    async fn complete(
        &mut self,
        message: &ServiceBusReceivedMessage,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        self.inner.complete(message, session_id).await
    }

    async fn defer(
        &mut self,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        self.inner
            .defer(message, properties_to_modify, session_id)
            .await
    }

    async fn peek_messages(
        &mut self,
        sequence_number: Option<i64>,
        message_count: i32,
    ) -> Result<Vec<ServiceBusPeekedMessage>, Self::RequestResponseError> {
        self.inner
            .peek_messages(sequence_number, message_count)
            .await
    }

    async fn peek_session_messages(
        &mut self,
        sequence_number: Option<i64>,
        message_count: i32,
        session_id: &str,
    ) -> Result<Vec<ServiceBusPeekedMessage>, Self::RequestResponseError> {
        self.inner
            .peek_session_messages(sequence_number, message_count, session_id)
            .await
    }

    async fn abandon(
        &mut self,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        self.inner
            .abandon(message, properties_to_modify, session_id)
            .await
    }

    async fn dead_letter(
        &mut self,
        message: &ServiceBusReceivedMessage,
        dead_letter_reason: Option<String>,
        dead_letter_error_description: Option<String>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        self.inner
            .dead_letter(
                message,
                dead_letter_reason,
                dead_letter_error_description,
                properties_to_modify,
                session_id,
            )
            .await
    }

    async fn receive_deferred_messages(
        &mut self,
        sequence_numbers: impl Iterator<Item = i64> + Send,
        session_id: Option<&str>,
    ) -> Result<Vec<ServiceBusReceivedMessage>, Self::RequestResponseError> {
        self.inner
            .receive_deferred_messages(sequence_numbers, session_id)
            .await
    }

    async fn renew_message_lock(
        &mut self,
        lock_token: Vec<fe2o3_amqp::types::primitives::Uuid>,
    ) -> Result<Vec<Timestamp>, Self::RequestResponseError> {
        self.inner.renew_message_lock(lock_token).await
    }
}

impl TransportSessionReceiver for AmqpSessionReceiver {
    fn session_id(&self) -> Option<&str> {
        self.inner.session_id()
    }

    fn session_locked_until(&self) -> OffsetDateTime {
        self.inner.session_locked_until()
    }

    fn set_session_locked_until(&mut self, session_locked_until: OffsetDateTime) {
        self.inner.set_session_locked_until(session_locked_until);
    }

    async fn renew_session_lock(
        &mut self,
        session_id: &str,
    ) -> Result<OffsetDateTime, Self::RequestResponseError> {
        let mut request =
            RenewSessionLockRequest::new(session_id, Some(self.inner.link_name.clone()));
        let mut try_timeout = self.inner.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.inner.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.renew_session_lock_inner(&mut request, &try_timeout),
            self.recover()
        )?;

        Ok(OffsetDateTime::from(response.expiration))
    }

    async fn session_state(
        &mut self,
        session_id: &str,
    ) -> Result<Vec<u8>, Self::RequestResponseError> {
        let mut request =
            GetSessionStateRequest::new(session_id, Some(self.inner.link_name.clone()));
        let mut try_timeout = self.inner.retry_policy.calculate_try_timeout(0);

        let response = run_operation!(
            { &self.inner.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.get_session_state_inner(&mut request, &try_timeout),
            self.recover()
        )?;

        Ok(response.session_state.into_vec())
    }

    async fn set_session_state(
        &mut self,
        session_id: &str,
        session_state: Vec<u8>,
    ) -> Result<(), Self::RequestResponseError> {
        let mut request = SetSessionStateRequest::new(
            session_id,
            Binary::from(session_state),
            Some(self.inner.link_name.clone()),
        );
        let mut try_timeout = self.inner.retry_policy.calculate_try_timeout(0);

        let _response = run_operation!(
            { &self.inner.retry_policy },
            AmqpRequestResponseError,
            try_timeout,
            self.set_session_state_inner(&mut request, &try_timeout),
            self.recover()
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod session_deadline_tests {
    use fe2o3_amqp_types::{definitions::Fields, primitives::Timestamp};
    use serde_amqp::Value;
    use time::{Duration, OffsetDateTime};

    use super::get_session_locked_until;
    use crate::amqp::amqp_client_constants::LOCKED_UNTIL_UTC;

    fn deadline(value: Value) -> Option<OffsetDateTime> {
        let mut properties = Fields::new();
        properties.insert(LOCKED_UNTIL_UTC.into(), value);
        get_session_locked_until(&Some(properties))
    }

    #[test]
    fn dotnet_tick_leases_preserve_epoch_precision_and_expiry() {
        const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
        for (ticks, expected) in [
            (
                UNIX_EPOCH_TICKS - 1,
                OffsetDateTime::UNIX_EPOCH - Duration::nanoseconds(100),
            ),
            (UNIX_EPOCH_TICKS, OffsetDateTime::UNIX_EPOCH),
            (
                UNIX_EPOCH_TICKS + 1,
                OffsetDateTime::UNIX_EPOCH + Duration::nanoseconds(100),
            ),
        ] {
            assert_eq!(deadline(Value::Long(ticks)), Some(expected));
        }
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(60);
        let expired = deadline(Value::Long(UNIX_EPOCH_TICKS + 590_000_000)).unwrap();
        let live = deadline(Value::Long(UNIX_EPOCH_TICKS + 610_000_000)).unwrap();
        assert!(
            expired < now,
            "an expired long lease must remain visible to settlement preflight"
        );
        assert!(live > now);
    }

    #[test]
    fn dotnet_tick_bounds_are_checked_without_panics_or_wrapping() {
        assert_eq!(
            deadline(Value::Long(0)),
            Some(time::macros::datetime!(0001-01-01 0:00 UTC))
        );
        assert_eq!(
            deadline(Value::Long(3_155_378_975_999_999_999)),
            Some(time::macros::datetime!(9999-12-31 23:59:59.9999999 UTC))
        );
        for ticks in [i64::MIN, -1, 3_155_378_976_000_000_000, i64::MAX] {
            assert_eq!(deadline(Value::Long(ticks)), None, "ticks: {ticks}");
        }
    }

    #[test]
    fn timestamp_leases_and_missing_or_malformed_properties_remain_supported() {
        assert_eq!(
            deadline(Value::Timestamp(Timestamp::from_milliseconds(42))),
            Some(OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(42))
        );
        assert_eq!(get_session_locked_until(&None), None);
        assert_eq!(get_session_locked_until(&Some(Fields::new())), None);
        for value in [
            Value::Null,
            Value::String("invalid".into()),
            Value::Ulong(u64::MAX),
            Value::Timestamp(Timestamp::from_milliseconds(i64::MIN)),
            Value::Timestamp(Timestamp::from_milliseconds(i64::MAX)),
        ] {
            assert_eq!(deadline(value), None);
        }
    }
}
