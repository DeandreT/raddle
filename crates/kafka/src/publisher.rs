use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use raddle::SubscriptionName;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, EnvelopeLimits};
use rdkafka::message::{Header as NativeHeader, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::task::TaskTracker;

use crate::Error;
use crate::config::{ClientSettings, Limits, Topic};
use crate::context::QuietContext;
use crate::record::Record;

/// Positively observed Kafka delivery. It does not attest consumer execution or
/// replication beyond the topic's configured in-sync replica requirements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Publication {
    /// Assigned native partition.
    pub partition: i32,
    /// Accepted native offset.
    pub offset: i64,
}

/// Confirmed producer to configured aliases, with a bounded SDK queue.
pub struct Publisher {
    producer: Arc<FutureProducer<QuietContext>>,
    routes: BTreeMap<String, Topic>,
    limits: Limits,
    admission: Semaphore,
    known_topics: RwLock<BTreeSet<String>>,
    metadata_admission: Arc<Semaphore>,
    tasks: TaskTracker,
}

impl Publisher {
    /// Connects without creating or deleting topics. A missing route is refused
    /// per publication rather than preventing unrelated configured routes from
    /// running. Application-owned topology is declared explicitly.
    pub async fn connect(
        settings: ClientSettings,
        routes: BTreeMap<String, Topic>,
    ) -> Result<Self, Error> {
        if routes.is_empty()
            || routes.len() > 1024
            || routes
                .keys()
                .any(|name| SubscriptionName::try_new(name).is_err())
        {
            return Err(Error::Configuration);
        }
        tokio::task::spawn_blocking(move || {
            let producer: FutureProducer<QuietContext> = settings
                .producer_config()
                .create_with_context(QuietContext)
                .map_err(|_| Error::Configuration)?;
            // A metadata request for all visible topics cannot accidentally
            // request creation of an unknown configured name.
            let metadata = producer
                .client()
                .fetch_metadata(None, Timeout::After(settings.limits.deadline))
                .map_err(|_| Error::Uncertain)?;
            let known_topics = metadata
                .topics()
                .iter()
                .filter(|topic| {
                    topic.error().is_none()
                        && routes.values().any(|route| route.as_str() == topic.name())
                })
                .map(|topic| topic.name().to_owned())
                .collect();
            Ok(Self {
                producer: Arc::new(producer),
                routes,
                limits: settings.limits,
                admission: Semaphore::new(settings.limits.max_pending),
                known_topics: RwLock::new(known_topics),
                metadata_admission: Arc::new(Semaphore::new(1)),
                tasks: TaskTracker::new(),
            })
        })
        .await
        .map_err(|_| Error::Native)?
    }

    /// Sends a raw record, including actual null tombstones and binary keys.
    /// SDK admission is not success: the final delivery report is awaited.
    pub async fn publish_record(
        &self,
        destination: &str,
        record: Record<'_>,
    ) -> Result<Publication, Error> {
        let topic = self.routes.get(destination).ok_or(Error::Configuration)?;
        let deadline = tokio::time::Instant::now() + self.limits.deadline;
        let _permit = self.admission.try_acquire().map_err(|_| Error::Capacity)?;
        record.validate(self.limits)?;
        tokio::time::timeout_at(deadline, self.ensure_topic(topic, deadline))
            .await
            .map_err(|_| Error::MissingTopic)??;
        let mut headers = OwnedHeaders::new();
        for header in record.headers {
            headers = headers.insert(NativeHeader {
                key: header.name(),
                value: header.value(),
            });
        }
        let mut native: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic.as_str()).headers(headers);
        if let Some(key) = record.key {
            native = native.key(key);
        }
        if let Some(value) = record.value {
            native = native.payload(value);
        }
        let delivered = tokio::time::timeout_at(
            deadline,
            self.producer.send(
                native,
                Timeout::After(deadline.saturating_duration_since(tokio::time::Instant::now())),
            ),
        )
        .await
        .map_err(|_| Error::Uncertain)?;
        let receipt = delivered.map_err(|_| Error::Uncertain)?;
        Ok(Publication {
            partition: receipt.partition,
            offset: receipt.offset,
        })
    }

    async fn ensure_topic(
        &self,
        topic: &Topic,
        deadline: tokio::time::Instant,
    ) -> Result<(), Error> {
        loop {
            if self.known_topics.read().await.contains(topic.as_str()) {
                return Ok(());
            }
            let permit = Arc::clone(&self.metadata_admission)
                .acquire_owned()
                .await
                .map_err(|_| Error::Native)?;
            if self.known_topics.read().await.contains(topic.as_str()) {
                return Ok(());
            }
            let producer = Arc::clone(&self.producer);
            let name = topic.as_str().to_owned();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::MissingTopic);
            }
            let timeout = remaining.min(Duration::from_secs(1));
            // Retain native-operation admission even if the async caller disappears.
            // Otherwise canceled metadata waits could create unlimited blocking work.
            let exists = self
                .tasks
                .spawn_blocking(move || {
                    let _permit = permit;
                    let metadata = producer
                        .client()
                        .fetch_metadata(Some(&name), Timeout::After(timeout))
                        .map_err(|_| Error::Uncertain)?;
                    Ok::<_, Error>(
                        metadata
                            .topics()
                            .iter()
                            .any(|topic| topic.name() == name && topic.error().is_none()),
                    )
                })
                .await
                .map_err(|_| Error::Native)??;
            if exists {
                self.known_topics
                    .write()
                    .await
                    .insert(topic.as_str().to_owned());
                return Ok(());
            }
            // Kafka can acknowledge topic creation before every broker's metadata
            // has caught up. Retry only this pre-send read, never a Produce request.
            // Each read releases native admission so another route can make progress.
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + Duration::from_millis(100)).min(deadline),
            )
            .await;
        }
    }

    /// Native key routing without changing the immutable envelope or headers.
    pub async fn publish_keyed(
        &self,
        destination: &str,
        envelope: &Envelope,
        key: Option<&[u8]>,
    ) -> Result<Publication, Error> {
        envelope
            .validate(EnvelopeLimits {
                max_bytes: self.limits.max_record_bytes,
                max_headers: self.limits.max_headers,
            })
            .map_err(|_| Error::Rejected)?;
        let bytes = envelope.to_vec().map_err(|_| Error::Rejected)?;
        self.publish_record(destination, Record::value(key, &bytes, &[]))
            .await
    }

    /// Flushes and destroys the native producer off the asynchronous worker.
    /// A deadline failure is uncertain; native SDK destruction itself is not
    /// cancellable and this method never claims dropping a future cancels sends.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.tasks.close();
        self.tasks.wait().await;
        tokio::task::spawn_blocking(move || {
            let result = self
                .producer
                .flush(Timeout::After(self.limits.deadline))
                .map_err(|_| Error::Uncertain);
            drop(self);
            result
        })
        .await
        .map_err(|_| Error::Native)?
    }
}

impl Transport for Publisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        match self.publish_keyed(destination, envelope, None).await {
            Ok(_) => Ok(()),
            Err(Error::Configuration | Error::Rejected) => Err(TransportError::Rejected),
            Err(_) => Err(TransportError::Transient),
        }
    }
}
