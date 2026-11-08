use std::time::Duration;

use prost::Message;
use raddle::wire::WireMessage;
use raddle::{Request, SubscriptionName};
use serde::de::DeserializeOwned;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::json::{self, EncodeError};
use crate::protocol::InvokeRequest;
use crate::protocol::dispatch_client::DispatchClient;
use crate::{ClientError, MAX_JSON_BYTES, MAX_MESSAGE_BYTES};

/// A typed unary helper over an application-created Tonic channel.
/// No connection, listener, credential refresh or retry occurs implicitly.
#[derive(Clone)]
pub struct GrpcClient {
    inner: DispatchClient<Channel>,
}

impl GrpcClient {
    /// Uses the application's channel, preserving its TLS and connection policy.
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: DispatchClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        }
    }

    /// Invokes one typed wire request with an explicit bearer and total deadline.
    /// Dropping the future cancels its HTTP/2 stream, not unrelated RPCs. A timeout
    /// or connection error does not prove the handler performed no side effects.
    pub async fn invoke<M>(
        &mut self,
        message: &M,
        bearer: &str,
        timeout: Duration,
    ) -> Result<M::Reply, ClientError>
    where
        M: Request + WireMessage,
        M::Reply: DeserializeOwned,
    {
        if bearer.is_empty()
            || bearer.len() > 4096
            || !bearer.bytes().all(|byte| byte.is_ascii_graphic())
            || !(Duration::from_millis(1)..=Duration::from_secs(120)).contains(&timeout)
            || M::NAME.len() > 128
            || M::VERSION == 0
            || SubscriptionName::try_new(M::NAME).is_err()
        {
            return Err(ClientError::InvalidInput);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        let json = json::encode(message).map_err(|error| match error {
            EncodeError::TooLarge => ClientError::TooLarge,
            EncodeError::Serialization => ClientError::Serialization,
        })?;
        let message = InvokeRequest {
            contract: M::NAME.into(),
            version: M::VERSION,
            json,
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(ClientError::Rpc(Status::deadline_exceeded(
                "RPC deadline exceeded",
            )));
        }
        if message.encoded_len() > MAX_MESSAGE_BYTES {
            return Err(ClientError::TooLarge);
        }
        let mut request = tonic::Request::new(message);
        let mut credential = MetadataValue::try_from(format!("Bearer {bearer}"))
            .map_err(|_| ClientError::InvalidInput)?;
        credential.set_sensitive(true);
        request.metadata_mut().insert("authorization", credential);
        request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
        let reply = tokio::time::timeout_at(deadline, self.inner.invoke(request))
            .await
            .map_err(|_| ClientError::Rpc(Status::deadline_exceeded("RPC deadline exceeded")))?
            .map_err(ClientError::Rpc)?
            .into_inner();
        if reply.json.len() > MAX_JSON_BYTES {
            return Err(ClientError::TooLarge);
        }
        serde_json::from_slice(&reply.json).map_err(|_| ClientError::InvalidResponse)
    }
}
