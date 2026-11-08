use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;

use crate::Error;
use crate::config::{ClientSettings, Limits, Topic};
use crate::context::QuietContext;

#[derive(Clone)]
enum Management {
    External,
    Managed {
        partitions: i32,
        replication: i32,
        config: BTreeMap<String, String>,
    },
}

/// Explicit ownership. External resources participate in neither setup nor
/// teardown mutation; application-managed topics are never inferred from data.
#[derive(Clone)]
pub struct TopicSpec {
    topic: Topic,
    management: Management,
}

impl TopicSpec {
    /// Describes an existing external topic without provisioning authority.
    pub fn external(topic: Topic) -> Self {
        Self {
            topic,
            management: Management::External,
        }
    }

    /// Describes an explicitly application-managed topic.
    pub fn managed(topic: Topic, partitions: u16, replication: u16) -> Result<Self, Error> {
        if !(1..=1024).contains(&partitions) || !(1..=16).contains(&replication) {
            return Err(Error::Configuration);
        }
        Ok(Self {
            topic,
            management: Management::Managed {
                partitions: i32::from(partitions),
                replication: i32::from(replication),
                config: BTreeMap::new(),
            },
        })
    }

    /// Adds a bounded native topic setting only to application-managed topics.
    pub fn option(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let value = value.into();
        if name.is_empty()
            || name.len() > 256
            || value.len() > 4096
            || name.contains('\0')
            || value.contains('\0')
        {
            return Err(Error::Configuration);
        }
        let Management::Managed { config, .. } = &mut self.management else {
            return Err(Error::Configuration);
        };
        if config.len() >= 64 && !config.contains_key(&name) {
            return Err(Error::Configuration);
        }
        config.insert(name, value);
        Ok(self)
    }

    /// Configured native topic name.
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
}

/// One resource's observed outcome; successful siblings survive another failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TopologyOutcome {
    /// Broker confirmed topic creation.
    Created,
    /// The managed topic already existed; its settings were not silently changed.
    AlreadyPresent,
    /// External resource was observed, without mutation.
    ExternalPresent,
    /// Explicitly managed resource was deleted.
    Deleted,
    /// An already absent managed resource needed no further deletion.
    AlreadyAbsent,
    /// An external resource was excluded from destructive teardown.
    ExternalUntouched,
}

/// Per-topic topology result; never an all-or-nothing success announcement.
#[derive(Debug)]
pub struct TopologyResult {
    /// Application-configured topic.
    pub topic: Topic,
    /// Observed resource-specific outcome.
    pub result: Result<TopologyOutcome, Error>,
}

/// Explicit native topology operations. Nothing is provisioned by construction.
pub struct Admin {
    operations: Arc<Operations>,
    admission: Arc<Semaphore>,
    tasks: TaskTracker,
}

struct Operations {
    client: Arc<AdminClient<QuietContext>>,
    limits: Limits,
    tasks: TaskTracker,
}

impl Admin {
    /// Inherits the same broker, credentials and bounds as producer/consumer.
    pub async fn connect(settings: &ClientSettings) -> Result<Self, Error> {
        let config = settings.native.clone();
        let limits = settings.limits;
        tokio::task::spawn_blocking(move || {
            let client = config
                .create_with_context(QuietContext)
                .map_err(|_| Error::Configuration)?;
            let tasks = TaskTracker::new();
            Ok(Self {
                operations: Arc::new(Operations {
                    client: Arc::new(client),
                    limits,
                    tasks: tasks.clone(),
                }),
                admission: Arc::new(Semaphore::new(limits.max_pending.min(16))),
                tasks,
            })
        })
        .await
        .map_err(|_| Error::Native)?
    }

    fn validate(specs: &[TopicSpec]) -> Result<(), Error> {
        if specs.is_empty() || specs.len() > 256 {
            return Err(Error::Configuration);
        }
        let mut names = BTreeSet::new();
        for spec in specs {
            if !names.insert(spec.topic.as_str()) {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }

    /// Creates only explicitly managed resources, retaining each topic's outcome.
    /// A canceled caller does not cancel an already issued native admin request.
    pub async fn declare(&self, specs: &[TopicSpec]) -> Result<Vec<TopologyResult>, Error> {
        self.execute(specs, false).await
    }

    /// Deletes only explicitly managed topics. External resources are untouched.
    pub async fn delete_managed(&self, specs: &[TopicSpec]) -> Result<Vec<TopologyResult>, Error> {
        self.execute(specs, true).await
    }

    async fn execute(
        &self,
        specs: &[TopicSpec],
        delete: bool,
    ) -> Result<Vec<TopologyResult>, Error> {
        Self::validate(specs)?;
        let bytes: usize = specs
            .iter()
            .map(|spec| {
                spec.topic.as_str().len()
                    + match &spec.management {
                        Management::External => 0,
                        Management::Managed { config, .. } => config
                            .iter()
                            .map(|(key, value)| key.len() + value.len())
                            .sum(),
                    }
            })
            .sum();
        if bytes > self.operations.limits.max_record_bytes {
            return Err(Error::Configuration);
        }
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let operations = Arc::clone(&self.operations);
        // The independent copy and admission remain owned by a tracked native
        // operation even when its original caller cancels its wait.
        let specs = specs.to_vec();
        let operation = self.tasks.spawn(async move {
            let _permit = permit;
            if delete {
                operations.delete_managed(&specs).await
            } else {
                operations.declare(&specs).await
            }
        });
        tokio::time::timeout(self.operations.limits.deadline, operation)
            .await
            .map_err(|_| Error::Uncertain)?
            .map_err(|_| Error::Native)?
    }

    /// Joins admitted operations, then destroys native workers off the async
    /// worker. Native SDK shutdown is not a cancellable hard-deadline guarantee.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.tasks.close();
        self.tasks.wait().await;
        tokio::task::spawn_blocking(move || drop(self))
            .await
            .map_err(|_| Error::Native)
    }
}

impl Operations {
    /// Creates only explicitly managed resources. Existing managed topics are
    /// reported, not silently reconfigured. External validation is read-only.
    async fn declare(&self, specs: &[TopicSpec]) -> Result<Vec<TopologyResult>, Error> {
        let deadline = tokio::time::Instant::now() + self.limits.deadline;
        let mut results: BTreeMap<String, Result<TopologyOutcome, Error>> = BTreeMap::new();
        let topics: Vec<_> = specs
            .iter()
            .filter_map(|spec| match &spec.management {
                Management::External => None,
                Management::Managed {
                    partitions,
                    replication,
                    config,
                } => {
                    let mut topic = NewTopic::new(
                        spec.topic.as_str(),
                        *partitions,
                        TopicReplication::Fixed(*replication),
                    );
                    for (name, value) in config {
                        topic = topic.set(name, value);
                    }
                    Some(topic)
                }
            })
            .collect();
        if !topics.is_empty() {
            let options = AdminOptions::new()
                .operation_timeout(Some(self.limits.deadline))
                .request_timeout(Some(self.limits.deadline));
            let created = self.client.create_topics(&topics, &options).await;
            if let Ok(created) = created {
                for result in created {
                    match result {
                        Ok(name) => {
                            results.insert(name, Ok(TopologyOutcome::Created));
                        }
                        Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                            results.insert(name, Ok(TopologyOutcome::AlreadyPresent));
                        }
                        Err((name, _)) => {
                            results.insert(name, Err(Error::Uncertain));
                        }
                    }
                }
            }
        }
        let external: Vec<_> = specs
            .iter()
            .filter(|spec| matches!(spec.management, Management::External))
            .map(|spec| spec.topic.as_str().to_owned())
            .collect();
        if !external.is_empty() {
            let client = Arc::clone(&self.client);
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let metadata = self
                .tasks
                .spawn_blocking(move || {
                    let data = client
                        .inner()
                        .fetch_metadata(None, Timeout::After(remaining))
                        .map_err(|_| Error::Uncertain)?;
                    Ok::<_, Error>(
                        data.topics()
                            .iter()
                            .filter(|topic| topic.error().is_none())
                            .map(|topic| topic.name().to_owned())
                            .collect::<BTreeSet<_>>(),
                    )
                })
                .await
                .map_err(|_| Error::Native)?;
            for name in external {
                let result = match &metadata {
                    Ok(names) if names.contains(&name) => Ok(TopologyOutcome::ExternalPresent),
                    Ok(_) => Err(Error::MissingTopic),
                    Err(_) => Err(Error::Uncertain),
                };
                results.insert(name, result);
            }
        }
        Ok(specs
            .iter()
            .map(|spec| TopologyResult {
                topic: spec.topic.clone(),
                result: results
                    .remove(spec.topic.as_str())
                    .unwrap_or(Err(Error::Uncertain)),
            })
            .collect())
    }

    /// Deletes only explicitly managed topics. Never use this with someone
    /// else's ownership declaration; external topics are always left untouched.
    async fn delete_managed(&self, specs: &[TopicSpec]) -> Result<Vec<TopologyResult>, Error> {
        let owned: Vec<_> = specs
            .iter()
            .filter(|spec| matches!(spec.management, Management::Managed { .. }))
            .map(|spec| spec.topic.as_str())
            .collect();
        let mut deleted = BTreeMap::new();
        if !owned.is_empty() {
            let options = AdminOptions::new()
                .operation_timeout(Some(self.limits.deadline))
                .request_timeout(Some(self.limits.deadline));
            if let Ok(results) = self.client.delete_topics(&owned, &options).await {
                for result in results {
                    match result {
                        Ok(name) => {
                            deleted.insert(name, Ok(TopologyOutcome::Deleted));
                        }
                        Err((name, RDKafkaErrorCode::UnknownTopicOrPartition)) => {
                            deleted.insert(name, Ok(TopologyOutcome::AlreadyAbsent));
                        }
                        Err((name, _)) => {
                            deleted.insert(name, Err(Error::Uncertain));
                        }
                    }
                }
            }
        }
        Ok(specs
            .iter()
            .map(|spec| TopologyResult {
                topic: spec.topic.clone(),
                result: match spec.management {
                    Management::External => Ok(TopologyOutcome::ExternalUntouched),
                    Management::Managed { .. } => deleted
                        .remove(spec.topic.as_str())
                        .unwrap_or(Err(Error::Uncertain)),
                },
            })
            .collect())
    }
}
