//! Owned recovery resources, independent of any particular receive future.

use std::sync::Arc;

use fe2o3_amqp::{link::ReceiverAttachExchange, Receiver};
use fe2o3_amqp_types::messaging::{Body, Modified};
use serde_amqp::Value;
use tokio::sync::{mpsc, Mutex};
use url::Url;

use super::{
    amqp_cbs_link,
    amqp_connection_scope::{AmqpConnectionScope, ReceiverType},
    amqp_management_link::AmqpManagementLink,
    error::RecoverReceiverError,
};
use crate::{authorization::service_bus_claim, core::RecoverableTransport, ServiceBusReceiveMode};

#[derive(Debug)]
pub(super) struct ReceiverRecovery {
    pub id: u32,
    pub receiver: Option<Receiver>,
    pub management_link: Option<AmqpManagementLink>,
    pub cbs_command_sender: mpsc::Sender<amqp_cbs_link::Command>,
    pub service_endpoint: Arc<Url>,
    pub entity_path: String,
    pub identifier: String,
    pub receive_mode: ServiceBusReceiveMode,
    pub receiver_type: ReceiverType,
    pub prefetch_count: u32,
    pub connection_scope: Arc<Mutex<AmqpConnectionScope>>,
}

impl ReceiverRecovery {
    pub async fn run(&mut self) -> Result<(), RecoverReceiverError> {
        let mut scope = self.connection_scope.lock().await;
        scope.recover().await?;
        let endpoint = format!("{}/{}", self.service_endpoint, self.entity_path);
        scope
            .request_refreshable_authorization_using_cbs(
                self.id,
                endpoint.clone(),
                endpoint,
                vec![service_bus_claim::SEND.to_owned()],
            )
            .await?;

        if let Some(receiver) = &mut self.receiver {
            let mut exchange = receiver
                .detach_then_resume_on_session(&scope.session.handle)
                .await?;
            while let ReceiverAttachExchange::IncompleteUnsettled = exchange {
                match receiver.recv::<Body<Value>>().await {
                    Ok(delivery) => {
                        if let Err(error) = receiver
                            .modify(
                                delivery,
                                Modified {
                                    delivery_failed: None,
                                    undeliverable_here: None,
                                    message_annotations: None,
                                },
                            )
                            .await
                        {
                            log::warn!("Failed to release resumed delivery: {error}");
                        }
                    }
                    Err(error) => log::warn!("Error receiving resumed delivery: {error}"),
                }
                exchange = receiver
                    .detach_then_resume_on_session(&scope.session.handle)
                    .await?;
            }
        } else {
            let _ = self
                .cbs_command_sender
                .send(amqp_cbs_link::Command::RemoveAuthorizationRefresher(
                    self.id,
                ))
                .await;
            let (id, receiver, cbs) = scope
                .open_receiver_link(
                    &self.service_endpoint,
                    &self.entity_path,
                    &self.identifier,
                    &self.receive_mode,
                    self.receiver_type.clone(),
                    self.prefetch_count,
                )
                .await?;
            self.id = id;
            self.receiver = Some(receiver);
            self.cbs_command_sender = cbs;
        }

        self.management_link = Some(
            scope
                .open_management_link(&self.service_endpoint, &self.entity_path, &self.identifier)
                .await?,
        );
        self.cbs_command_sender = scope.cbs_link.command_sender().clone();
        Ok(())
    }
}
