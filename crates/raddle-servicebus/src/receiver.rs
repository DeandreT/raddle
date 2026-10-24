use std::sync::Arc;
use std::time::Duration;

use azservicebus::receiver::DeadLetterOptions;
use azservicebus::{
    ServiceBusReceivedMessage, ServiceBusReceiver, ServiceBusReceiverOptions,
    ServiceBusSessionReceiver, ServiceBusSessionReceiverOptions, SubQueue,
};
use raddle::wire::{Envelope, EnvelopeLimits, WireError};
use uuid::Uuid;

use crate::connection::{self, SharedClient};
use crate::{Connection, Error};

/// A configured incoming entity; not obtained from untrusted message metadata.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Source {
    /// A primary queue.
    Queue(String),
    /// A named subscription under a topic.
    Subscription {
        /// Topic name.
        topic: String,
        /// Subscription name.
        subscription: String,
    },
    /// The dead-letter subqueue of an ordinary queue.
    DeadLetters(String),
    /// The dead-letter subqueue of a topic subscription.
    SubscriptionDeadLetters {
        /// Parent topic name.
        topic: String,
        /// Subscription name.
        subscription: String,
    },
}

enum Link {
    Ordinary(ServiceBusReceiver),
    Session(ServiceBusSessionReceiver),
}

/// A live lock bound to one receiver. Decoding does not settle or discard it.
pub struct HeldMessage {
    owner: Uuid,
    message: ServiceBusReceivedMessage,
    settled: bool,
}

impl HeldMessage {
    /// Validates the wire envelope without losing the lock on malformed input.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        let bytes = self
            .message
            .body()
            .map_err(|_| WireError::Invalid("unsupported AMQP body"))?;
        Envelope::from_slice(bytes, EnvelopeLimits::default())
    }
    /// Whether a successful disposition already consumed this lock.
    pub fn is_settled(&self) -> bool {
        self.settled
    }
}

/// Peek-lock receiver; callers commit custody before completing a held message.
pub struct Receiver {
    link: Link,
    owner: Uuid,
    _connection: SharedClient,
}

impl Receiver {
    pub(crate) async fn open(
        connection: &Connection,
        source: Source,
        session: Option<&str>,
        prefetch: u32,
    ) -> Result<Self, Error> {
        if prefetch > 256 {
            return Err(Error::Invalid("prefetch exceeds 256"));
        }
        let (entity, subscription, dead) = match source {
            Source::Queue(entity) => (entity, None, false),
            Source::Subscription {
                topic,
                subscription,
            } => (topic, Some(subscription), false),
            Source::DeadLetters(entity) => (entity, None, true),
            Source::SubscriptionDeadLetters {
                topic,
                subscription,
            } => (topic, Some(subscription), true),
        };
        connection::validate_entity(&entity)?;
        if let Some(subscription) = &subscription {
            connection::validate_entity(subscription)?;
        }
        if session.is_some_and(|id| id.is_empty() || id.encode_utf16().count() > 128)
            || (dead && session.is_some())
        {
            return Err(Error::Invalid("invalid session"));
        }
        let mut guard = connection.client.lock().await;
        let client = guard.as_mut().ok_or(Error::Closed)?;
        let link = if let Some(session) = session {
            let options = ServiceBusSessionReceiverOptions {
                prefetch_count: prefetch,
                ..Default::default()
            };
            let receiver = match subscription {
                Some(subscription) => {
                    Box::pin(client.accept_session_for_subscription(
                        &entity,
                        subscription,
                        session,
                        options,
                    ))
                    .await
                }
                None => Box::pin(client.accept_session_for_queue(entity, session, options)).await,
            }
            .map_err(|_| Error::Service)?;
            Link::Session(receiver)
        } else {
            let options = ServiceBusReceiverOptions {
                prefetch_count: prefetch,
                sub_queue: if dead {
                    SubQueue::DeadLetter
                } else {
                    SubQueue::None
                },
                ..Default::default()
            };
            let receiver = match subscription {
                Some(subscription) => {
                    Box::pin(client.create_receiver_for_subscription(
                        &entity,
                        subscription,
                        options,
                    ))
                    .await
                }
                None => Box::pin(client.create_receiver_for_queue(entity, options)).await,
            }
            .map_err(|_| Error::Service)?;
            Link::Ordinary(receiver)
        };
        Ok(Self {
            link,
            owner: Uuid::new_v4(),
            _connection: Arc::clone(&connection.client),
        })
    }

    /// Waits for a bounded batch. No message is settled merely by receiving it.
    pub async fn receive(
        &mut self,
        count: u32,
        max_wait: Duration,
    ) -> Result<Vec<HeldMessage>, Error> {
        if !(1..=256).contains(&count) || max_wait.is_zero() || max_wait > Duration::from_secs(60) {
            return Err(Error::Invalid("invalid receive bounds"));
        }
        let messages = match &mut self.link {
            Link::Ordinary(receiver) => {
                Box::pin(receiver.receive_messages_with_max_wait_time(count, max_wait)).await
            }
            Link::Session(receiver) => {
                Box::pin(receiver.receive_messages_with_max_wait_time(count, max_wait)).await
            }
        }
        .map_err(|_| Error::Service)?;
        Ok(messages
            .into_iter()
            .map(|message| HeldMessage {
                owner: self.owner,
                message,
                settled: false,
            })
            .collect())
    }

    fn check(&self, message: &HeldMessage) -> Result<(), Error> {
        if message.owner != self.owner || message.settled {
            return Err(Error::Invalid(
                "message is settled or belongs to another receiver",
            ));
        }
        Ok(())
    }

    /// Completes a message only after the caller has persisted custody or its effects.
    pub async fn complete(&mut self, message: &mut HeldMessage) -> Result<(), Error> {
        self.check(message)?;
        match &mut self.link {
            Link::Ordinary(receiver) => Box::pin(receiver.complete_message(&message.message)).await,
            Link::Session(receiver) => Box::pin(receiver.complete_message(&message.message)).await,
        }
        .map_err(|_| Error::Service)?;
        message.settled = true;
        Ok(())
    }

    /// Releases the lock for redelivery. A failed disposition retains local ownership.
    pub async fn abandon(&mut self, message: &mut HeldMessage) -> Result<(), Error> {
        self.check(message)?;
        match &mut self.link {
            Link::Ordinary(receiver) => {
                Box::pin(receiver.abandon_message(&message.message, None)).await
            }
            Link::Session(receiver) => {
                Box::pin(receiver.abandon_message(&message.message, None)).await
            }
        }
        .map_err(|_| Error::Service)?;
        message.settled = true;
        Ok(())
    }

    /// Sends malformed or permanently rejected input to the broker dead-letter queue.
    pub async fn dead_letter(
        &mut self,
        message: &mut HeldMessage,
        reason: &str,
    ) -> Result<(), Error> {
        self.check(message)?;
        if reason.is_empty() || reason.len() > 256 {
            return Err(Error::Invalid("invalid dead-letter reason"));
        }
        let options = DeadLetterOptions {
            dead_letter_reason: Some(reason.to_owned()),
            ..Default::default()
        };
        match &mut self.link {
            Link::Ordinary(receiver) => {
                Box::pin(receiver.dead_letter_message(&message.message, options)).await
            }
            Link::Session(receiver) => {
                Box::pin(receiver.dead_letter_message(&message.message, options)).await
            }
        }
        .map_err(|_| Error::Service)?;
        message.settled = true;
        Ok(())
    }

    /// Renews a live message lock. A lost/expired lock is an error, not a successful renewal.
    pub async fn renew(&mut self, message: &mut HeldMessage) -> Result<(), Error> {
        self.check(message)?;
        match &mut self.link {
            Link::Ordinary(receiver) => {
                Box::pin(receiver.renew_message_lock(&mut message.message)).await
            }
            Link::Session(receiver) => {
                Box::pin(receiver.renew_message_lock(&mut message.message)).await
            }
        }
        .map_err(|_| Error::Service)
    }

    /// Renews exclusive session ownership; ordinary receivers reject this operation.
    pub async fn renew_session(&mut self) -> Result<(), Error> {
        match &mut self.link {
            Link::Session(receiver) => Box::pin(receiver.renew_session_lock())
                .await
                .map_err(|_| Error::Service),
            Link::Ordinary(_) => Err(Error::Invalid("receiver has no session")),
        }
    }

    /// Closes this link; unsettled messages remain recoverable by the broker.
    pub async fn close(self) -> Result<(), Error> {
        match self.link {
            Link::Ordinary(receiver) => Box::pin(receiver.dispose()).await,
            Link::Session(receiver) => Box::pin(receiver.dispose()).await,
        }
        .map_err(|_| Error::Service)
    }
}
