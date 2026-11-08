//! Caller-polled cleanup operations; no detached task is created.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use tokio::time::Instant;

use super::Result;

pub(super) struct Operation<'a> {
    name: &'static str,
    future: Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
}

impl<'a> Operation<'a> {
    pub(super) fn new(
        name: &'static str,
        future: impl Future<Output = Result<()>> + Send + 'a,
    ) -> Self {
        Self {
            name,
            future: Box::pin(future),
        }
    }
}

pub(super) struct Budget {
    deadline: Instant,
    per_operation: Duration,
}
impl Budget {
    pub(super) fn new(total: Duration, per_operation: Duration) -> Result<Self> {
        if !(Duration::from_millis(1)..=Duration::from_secs(10)).contains(&total)
            || per_operation < Duration::from_millis(1)
            || per_operation > total
        {
            return Err("invalid cleanup budget".into());
        }
        Ok(Self {
            deadline: Instant::now() + total,
            per_operation,
        })
    }

    pub(super) async fn run(&self, operations: Vec<Operation<'_>>) -> Result<()> {
        if operations.len() > 8 {
            return Err("cleanup phase exceeds eight owned operations".into());
        }
        let mut remaining = operations.len();
        let mut failure = None;
        let mut pending: Vec<_> = operations
            .into_iter()
            .map(|operation| {
                let deadline = self.deadline.min(Instant::now() + self.per_operation);
                Some(Box::pin(async move {
                    tokio::time::timeout_at(deadline, operation.future)
                        .await
                        .map_err(|_| format!("cleanup operation timed out: {}", operation.name))?
                }))
            })
            .collect();
        // Poll every operation, even when an earlier one fails or the shared
        // deadline expired. Timeout drops only its own future, never detaches it.
        std::future::poll_fn(|context| {
            for slot in &mut pending {
                if let Some(future) = slot
                    && let Poll::Ready(result) = future.as_mut().poll(context)
                {
                    *slot = None;
                    remaining -= 1;
                    if let Err(error) = result
                        && failure.is_none()
                    {
                        failure = Some(error);
                    }
                }
            }
            if remaining == 0 {
                Poll::Ready(match failure.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                })
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::{Budget, Operation};

    #[tokio::test]
    async fn pending_cleanup_is_bounded_and_cannot_block_remaining_operations() {
        let attempted = AtomicBool::new(false);
        let budget = Budget::new(Duration::from_millis(40), Duration::from_millis(10)).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            budget.run(vec![
                Operation::new("pending first", std::future::pending()),
                Operation::new("remaining operation", async {
                    attempted.store(true, Ordering::SeqCst);
                    Ok(())
                }),
            ]),
        )
        .await;
        assert!(
            result.is_ok(),
            "cleanup plan exceeded its enforced deadline"
        );
        assert!(result.unwrap().is_err());
        assert!(attempted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shared_outer_deadline_is_not_reset_between_phases() {
        let budget = Budget::new(Duration::from_millis(10), Duration::from_millis(10)).unwrap();
        assert!(
            budget
                .run(vec![Operation::new("first phase", std::future::pending())])
                .await
                .is_err()
        );
        assert!(tokio::time::Instant::now() >= budget.deadline);
        let attempted = AtomicBool::new(false);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            budget.run(vec![
                Operation::new("expired phase pending", std::future::pending()),
                Operation::new("expired phase remaining", async {
                    attempted.store(true, Ordering::SeqCst);
                    Ok(())
                }),
            ]),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(attempted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_operation_does_not_skip_later_cleanup() {
        let attempted = AtomicBool::new(false);
        let budget = Budget::new(Duration::from_millis(40), Duration::from_millis(10)).unwrap();
        assert!(
            budget
                .run(vec![
                    Operation::new("failure", async { Err("injected cleanup failure".into()) }),
                    Operation::new("remaining", async {
                        attempted.store(true, Ordering::SeqCst);
                        Ok(())
                    }),
                ])
                .await
                .is_err()
        );
        assert!(attempted.load(Ordering::SeqCst));
    }
}
