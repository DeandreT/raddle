use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::{Id, JoinSet};

use crate::{PanicPolicy, RestartPolicy, ServiceContext, ServiceFailure, ServiceState, Status};

type ServiceFuture = Pin<Box<dyn Future<Output = Result<(), ServiceFailure>> + Send>>;
type Factory = Arc<dyn Fn(ServiceContext) -> ServiceFuture + Send + Sync>;

/// Configuration, terminal service failure, or expired cooperative shutdown.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HostError {
    /// Rejected before starting any service.
    #[error("invalid host configuration: {0}")]
    Invalid(&'static str),
    /// A permanent failure, panic, or exhausted restart budget stopped the host.
    #[error("service {name} failed: {failure:?}")]
    ServiceFailed {
        /// Validated service name.
        name: String,
        /// Sanitized terminal classification.
        failure: ServiceFailure,
    },
    /// Remaining tasks were aborted and joined after the grace period expired.
    #[error("shutdown deadline expired; {aborted} tasks aborted and joined")]
    ShutdownTimeout {
        /// Number of tasks still owned at the deadline.
        aborted: usize,
    },
}

struct Service {
    factory: Factory,
    policy: RestartPolicy,
    status: watch::Sender<Status>,
}

/// Readiness is true only while running and every service explicitly reports it.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Whether run is active and has not begun shutdown.
    pub running: bool,
    /// All configured services ready; false for an empty host.
    pub ready: bool,
    /// Independent per-service samples, in registration order.
    pub services: Vec<Status>,
}

/// Control and observation without ownership of service tasks.
#[derive(Clone)]
pub struct HostHandle {
    shutdown: watch::Sender<bool>,
    running: watch::Receiver<bool>,
    services: watch::Receiver<Vec<watch::Receiver<Status>>>,
}

impl HostHandle {
    /// Requests cooperative shutdown; callers must still await `Host::run`.
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    /// Samples service state; this is not a transactional cross-service snapshot.
    pub fn snapshot(&self) -> Snapshot {
        let running = *self.running.borrow() && !*self.shutdown.borrow();
        let services: Vec<_> = self
            .services
            .borrow()
            .iter()
            .map(|s| s.borrow().clone())
            .collect();
        let ready = running
            && !services.is_empty()
            && services.iter().all(|s| s.state == ServiceState::Ready);
        Snapshot {
            running,
            ready,
            services,
        }
    }
}

/// Owns at most 256 service factories. No tasks start during registration.
pub struct Host {
    services: Vec<Service>,
    grace: Duration,
    shutdown: watch::Sender<bool>,
    running: watch::Sender<bool>,
    observers: watch::Sender<Vec<watch::Receiver<Status>>>,
}

impl Host {
    /// Sets a finite cooperative grace period in 1ms..=300s.
    pub fn new(grace: Duration) -> Result<Self, HostError> {
        if !(Duration::from_millis(1)..=Duration::from_secs(300)).contains(&grace) {
            return Err(HostError::Invalid("invalid shutdown grace period"));
        }
        Ok(Self {
            services: Vec::new(),
            grace,
            shutdown: watch::channel(false).0,
            running: watch::channel(false).0,
            observers: watch::channel(Vec::new()).0,
        })
    }

    /// Registers a unique validated name and restartable factory.
    /// Each call to the factory receives a fresh not-ready context.
    pub fn service<F, Fut>(
        &mut self,
        name: &str,
        policy: RestartPolicy,
        factory: F,
    ) -> Result<(), HostError>
    where
        F: Fn(ServiceContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ServiceFailure>> + Send + 'static,
    {
        raddle::SubscriptionName::try_new(name)
            .map_err(|_| HostError::Invalid("invalid service name"))?;
        if self.services.len() >= 256 {
            return Err(HostError::Invalid("too many services"));
        }
        if self.services.iter().any(|s| s.status.borrow().name == name) {
            return Err(HostError::Invalid("duplicate service name"));
        }
        let status = Status {
            name: name.to_owned(),
            state: ServiceState::Registered,
            attempt: 0,
        };
        let status = watch::channel(status).0;
        self.observers
            .send_modify(|entries| entries.push(status.subscribe()));
        self.services.push(Service {
            factory: Arc::new(move |context| Box::pin(factory(context))),
            policy,
            status,
        });
        Ok(())
    }

    /// Creates a control handle which also observes subsequently added services.
    pub fn handle(&self) -> HostHandle {
        HostHandle {
            shutdown: self.shutdown.clone(),
            running: self.running.subscribe(),
            services: self.observers.subscribe(),
        }
    }

    /// Starts services when polled and returns only after every owned task joins.
    ///
    /// Transient failures consume finite restart budgets; permanent failures and
    /// exhausted budgets stop siblings cooperatively. Success is not restarted.
    /// An empty host returns immediately. Dropping this future requests shutdown
    /// and aborts owned tasks, but cannot asynchronously join: await it for joined
    /// shutdown. Services must yield; blocking code can prevent even abort/join.
    pub async fn run(self) -> Result<(), HostError> {
        let mut shutdown = self.shutdown.subscribe();
        let _guard = RunGuard {
            shutdown: self.shutdown.clone(),
            running: self.running.clone(),
            statuses: self.services.iter().map(|s| s.status.clone()).collect(),
        };
        if *shutdown.borrow() {
            return Ok(());
        }
        self.running.send_replace(true);
        let mut tasks = JoinSet::new();
        let mut owners = HashMap::new();
        for (index, service) in self.services.iter().enumerate() {
            let id = spawn(&mut tasks, service, shutdown.clone(), 1, Duration::ZERO);
            owners.insert(id, index);
        }
        let mut failure = None;
        while !tasks.is_empty() {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                joined = tasks.join_next_with_id() => {
                    let Some(joined) = joined else { break; };
                    let (id, outcome) = match joined {
                        Ok((id, result)) => (id, result),
                        Err(error) => (error.id(), Err(ServiceFailure::Panicked)),
                    };
                    let Some(index) = owners.remove(&id) else { continue; };
                    let service = &self.services[index];
                    let attempt = service.status.borrow().attempt;
                    let Err(reason) = outcome else {
                        service.status.send_modify(|s| s.state = ServiceState::Stopped);
                        continue;
                    };
                    let retryable = reason == ServiceFailure::Transient
                        || (reason == ServiceFailure::Panicked && service.policy.panics == PanicPolicy::Restart);
                    if retryable && attempt <= service.policy.maximum {
                        service.status.send_modify(|s| s.state = ServiceState::Backoff);
                        let id = spawn(&mut tasks, service, shutdown.clone(), attempt + 1, service.policy.delay(attempt));
                        owners.insert(id, index);
                    } else {
                        service.status.send_modify(|s| s.state = ServiceState::Failed);
                        failure = Some(HostError::ServiceFailed { name: service.status.borrow().name.clone(), failure: reason });
                        break;
                    }
                }
            }
        }
        self.running.send_replace(false);
        self.shutdown.send_replace(true);
        for service in &self.services {
            service.status.send_modify(|s| {
                if !matches!(s.state, ServiceState::Stopped | ServiceState::Failed) {
                    s.state = ServiceState::Stopping;
                }
            });
        }
        let deadline = tokio::time::sleep(self.grace);
        tokio::pin!(deadline);
        let mut aborted = 0;
        while !tasks.is_empty() {
            tokio::select! {
                biased;
                _ = &mut deadline, if aborted == 0 => {
                    aborted = tasks.len();
                    tasks.abort_all();
                }
                joined = tasks.join_next_with_id() => {
                    let Some(joined) = joined else { break; };
                    let (id, outcome) = match joined {
                        Ok((id, result)) => (id, result),
                        Err(error) if error.is_cancelled() && aborted > 0 => {
                            owners.remove(&error.id());
                            continue;
                        }
                        Err(error) => (error.id(), Err(ServiceFailure::Panicked)),
                    };
                    if let Some(index) = owners.remove(&id)
                        && let Err(reason) = outcome {
                        let service = &self.services[index];
                        service.status.send_modify(|s| s.state = ServiceState::Failed);
                        if failure.is_none() {
                            failure = Some(HostError::ServiceFailed {
                                name: service.status.borrow().name.clone(), failure: reason,
                            });
                        }
                    }
                }
            }
        }
        if let Some(error) = failure {
            Err(error)
        } else if aborted > 0 {
            Err(HostError::ShutdownTimeout { aborted })
        } else {
            Ok(())
        }
    }
}

fn spawn(
    tasks: &mut JoinSet<Result<(), ServiceFailure>>,
    service: &Service,
    shutdown: watch::Receiver<bool>,
    attempt: u8,
    delay: Duration,
) -> Id {
    service.status.send_modify(|s| {
        s.attempt = attempt;
        s.state = if delay.is_zero() {
            ServiceState::Starting
        } else {
            ServiceState::Backoff
        };
    });
    let context = ServiceContext {
        shutdown,
        status: service.status.clone(),
        attempt,
    };
    let factory = Arc::clone(&service.factory);
    tasks
        .spawn(async move {
            let _attempt_guard = AttemptGuard {
                status: context.status.clone(),
                attempt,
            };
            if *context.shutdown.borrow() {
                return Ok(());
            }
            if !delay.is_zero() {
                let mut context = context;
                tokio::select! {
                    biased;
                    _ = context.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(delay) => {}
                }
                context.status.send_modify(|s| {
                    if s.attempt == attempt
                        && s.state == ServiceState::Backoff
                        && !*context.shutdown.borrow()
                    {
                        s.state = ServiceState::Starting;
                    }
                });
                if *context.shutdown.borrow() {
                    return Ok(());
                }
                factory(context).await
            } else {
                factory(context).await
            }
        })
        .id()
}

struct AttemptGuard {
    status: watch::Sender<Status>,
    attempt: u8,
}
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.status.send_modify(|status| {
            if status.attempt == self.attempt && status.state != ServiceState::Failed {
                status.state = ServiceState::Stopped;
            }
        });
    }
}

struct RunGuard {
    shutdown: watch::Sender<bool>,
    running: watch::Sender<bool>,
    statuses: Vec<watch::Sender<Status>>,
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.running.send_replace(false);
        for status in &self.statuses {
            status.send_modify(|s| {
                if s.state != ServiceState::Failed {
                    s.state = ServiceState::Stopped;
                }
            });
        }
    }
}
