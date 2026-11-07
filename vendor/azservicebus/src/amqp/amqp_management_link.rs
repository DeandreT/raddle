use fe2o3_amqp::link::DetachError;
use fe2o3_amqp_management::MgmtClient;
use fe2o3_amqp_management::{error::Error, request::Request, response::Response};
use fe2o3_amqp_types::messaging::{
    message::__private::{Deserializable, Serializable},
    Body, Message, MessageId, Properties,
};
use serde_amqp::Value;
use tokio::sync::mpsc;

use super::amqp_cbs_link;

#[derive(Debug)]
pub(crate) struct AmqpManagementLink {
    identifier: u32,
    client: MgmtClient,
    cbs_command_sender: mpsc::Sender<amqp_cbs_link::Command>,
}

impl AmqpManagementLink {
    pub(crate) fn new(
        identifier: u32,
        client: MgmtClient,
        cbs_command_sender: mpsc::Sender<amqp_cbs_link::Command>,
    ) -> Self {
        Self {
            identifier,
            client,
            cbs_command_sender,
        }
    }

    pub(crate) async fn call<Req, Res>(&mut self, request: Req) -> Result<Res, Error>
    where
        Req: Request<Response = Res>,
        Res: Response,
        Res::Error: Into<Error>,
    {
        // Never reuse an ID after cancellation, timeout, retry, or reopening.
        let id = MessageId::String(uuid::Uuid::new_v4().to_string());
        self.client
            .send_request(CorrelatedRequest {
                request,
                id: id.clone(),
            })
            .await?
            .accepted_or_else(Error::NotAccepted)?;
        correlated_response(&id, async || {
            self.client.recv_response::<RawResponse>().await
        })
        .await
    }

    pub(crate) async fn close(self) -> Result<(), DetachError> {
        let _ = self
            .cbs_command_sender
            .send(amqp_cbs_link::Command::RemoveAuthorizationRefresher(
                self.identifier,
            ))
            .await;
        self.client.close().await
    }
}

struct CorrelatedRequest<R> {
    request: R,
    id: MessageId,
}

impl<R: Request> Request for CorrelatedRequest<R> {
    const OPERATION: &'static str = R::OPERATION;
    type Response = R::Response;
    type Body = R::Body;

    fn encode_body(self) -> Self::Body {
        self.request.encode_body()
    }

    fn into_message(self) -> Message<Self::Body> {
        let mut message = self.request.into_message();
        message
            .properties
            .get_or_insert_with(Properties::default)
            .message_id = Some(self.id);
        message
    }
}

struct RawResponse(Message<Body<Value>>);

impl Response for RawResponse {
    const STATUS_CODE: u16 = 200;
    type Body = Body<Value>;
    type Error = Error;

    fn decode_message(message: Message<Self::Body>) -> Result<Self, Error> {
        Ok(Self(message))
    }

    fn from_message(message: Message<Self::Body>) -> Result<Self, Error> {
        // Status and typed-body decoding must happen only after correlation.
        Ok(Self(message))
    }
}

async fn correlated_response<Res>(
    id: &MessageId,
    mut receive: impl AsyncFnMut() -> Result<RawResponse, Error>,
) -> Result<Res, Error>
where
    Res: Response,
    Res::Error: Into<Error>,
{
    loop {
        let RawResponse(message) = receive().await?;
        if message
            .properties
            .as_ref()
            .and_then(|properties| properties.correlation_id.as_ref())
            != Some(id)
        {
            continue;
        }
        let bytes =
            serde_amqp::to_vec(&Serializable(message)).map_err(|_| Error::DecodeError(None))?;
        let Deserializable(message): Deserializable<Message<Res::Body>> =
            serde_amqp::from_slice(&bytes).map_err(|_| Error::DecodeError(None))?;
        return Res::from_message(message).map_err(Into::into);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        correlated_response, Body, Error, Message, MessageId, Properties, RawResponse, Value,
    };
    use crate::amqp::amqp_response_message::update_disposition::UpdateDispositionResponse;
    use fe2o3_amqp_types::messaging::{AmqpValue, ApplicationProperties};
    use std::{collections::VecDeque, time::Duration};

    fn response(id: Option<&str>, status: i32) -> RawResponse {
        RawResponse(
            Message::builder()
                .properties(Properties {
                    correlation_id: id.map(|id| MessageId::String(id.into())),
                    ..Default::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("status-code", status)
                        .build(),
                )
                .body(Body::<Value>::Empty)
                .build(),
        )
    }

    #[tokio::test]
    async fn cancelled_request_late_success_cannot_confirm_a_later_refusal() {
        let old = MessageId::String("cancelled".into());
        let cancelled = tokio::time::timeout(
            Duration::from_millis(1),
            correlated_response::<UpdateDispositionResponse>(&old, async || {
                std::future::pending::<Result<RawResponse, Error>>().await
            }),
        )
        .await;
        assert!(cancelled.is_err());
        let mut responses = VecDeque::from([
            response(Some("cancelled"), 200),
            response(None, 200),
            response(Some("current"), 410),
        ]);
        let result = correlated_response::<UpdateDispositionResponse>(
            &MessageId::String("current".into()),
            async || Ok(responses.pop_front().unwrap()),
        )
        .await;
        assert!(matches!(result, Err(Error::Status(_))));
        assert!(responses.is_empty());
    }

    #[tokio::test]
    async fn unrelated_malformed_body_is_discarded_before_typed_status_decoding() {
        let mut old = response(Some("old"), 500);
        old.0.body = Body::Value(AmqpValue(Value::String(
            "not a disposition response".into(),
        )));
        let mut responses = VecDeque::from([old, response(Some("current"), 200)]);
        assert!(correlated_response::<UpdateDispositionResponse>(
            &MessageId::String("current".into()),
            async || Ok(responses.pop_front().unwrap()),
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn matching_response_retains_typed_map_bodies() {
        use crate::amqp::amqp_response_message::renew_lock::RenewLockResponse;
        use fe2o3_amqp_types::primitives::{Array, OrderedMap, Timestamp};
        let mut fields = OrderedMap::new();
        fields.insert(
            Value::String("expirations".into()),
            Value::Array(Array(vec![Value::Timestamp(Timestamp::from_milliseconds(
                42,
            ))])),
        );
        let mut raw = response(Some("current"), 200);
        raw.0.body = Body::Value(AmqpValue(Value::Map(fields)));
        let mut responses = VecDeque::from([raw]);
        let result = correlated_response::<RenewLockResponse>(
            &MessageId::String("current".into()),
            async || Ok(responses.pop_front().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(result.expirations.0[0].milliseconds(), 42);
    }

    #[test]
    fn correlation_id_preserves_the_original_management_request() {
        use crate::{
            amqp::amqp_request_message::update_disposition::UpdateDispositionRequest,
            primitives::disposition_status::DispositionStatus,
        };
        use fe2o3_amqp_management::request::Request;
        use fe2o3_amqp_types::primitives::{Array, Uuid};
        let request = UpdateDispositionRequest::new(
            DispositionStatus::Completed,
            Array(vec![Uuid::from([1; 16])]),
            None,
            None,
            None,
            None,
            Some("link".into()),
        );
        let id = MessageId::String("request-id".into());
        let message = super::CorrelatedRequest {
            request,
            id: id.clone(),
        }
        .into_message();
        assert_eq!(message.properties.unwrap().message_id, Some(id));
        assert_eq!(
            message.body.get("disposition-status"),
            Some(&Value::String("completed".into()))
        );
        assert!(message
            .application_properties
            .unwrap()
            .get("operation")
            .is_some());
    }
}
