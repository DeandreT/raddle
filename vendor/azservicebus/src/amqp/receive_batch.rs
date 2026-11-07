//! Receive helpers keep acquired deliveries visible across failures and deadlines.

use crate::util::time::Instant;
use fe2o3_amqp::Receiver;
use futures_util::FutureExt;
use std::time::Duration;

#[derive(Debug, Default)]
pub(super) struct Recovery {
    pub needed: bool,
    retry_at: Option<Instant>,
}

impl Recovery {
    pub fn failed(&mut self, delay: Option<Duration>) {
        self.needed = true;
        self.retry_at = delay.map(|delay| Instant::now() + delay);
    }

    pub fn remaining(&self) -> Duration {
        self.retry_at
            .map(|at| at.saturating_duration_since(Instant::now()))
            .unwrap_or_default()
    }

    pub fn recovered(&mut self) {
        *self = Self::default();
    }

    pub fn finish<T, E>(&self, result: Result<Vec<T>, E>, pending: E) -> Result<Vec<T>, E> {
        if self.needed && result.as_ref().is_ok_and(|buffer| buffer.is_empty()) {
            Err(pending)
        } else {
            result
        }
    }
}

pub(super) trait StopCredit {
    fn stop_credit(&mut self) -> bool;
}

impl StopCredit for Receiver {
    fn stop_credit(&mut self) -> bool {
        matches!(self.set_credit(0).now_or_never(), Some(Ok(())))
    }
}

/// Revokes manual credit even when a receive is cancelled by its outer deadline.
/// If the zero-credit flow cannot be enqueued immediately, drop/invalidate the
/// link. No cleanup task or outstanding grant survives as a reusable local link.
pub(super) struct CreditLease<'a, T: StopCredit> {
    pub slot: &'a mut Option<T>,
    pub recovery: &'a mut Recovery,
    pub manual: bool,
}

impl<T: StopCredit> Drop for CreditLease<'_, T> {
    fn drop(&mut self) {
        if self.manual && !self.slot.as_mut().is_some_and(StopCredit::stop_credit) {
            self.slot.take();
            self.recovery.needed = true;
        }
    }
}

/// Fill only the remaining space, including when an earlier attempt made progress.
pub(super) async fn fill<T, E>(
    buffer: &mut Vec<T>,
    max_messages: u32,
    mut receive: impl AsyncFnMut() -> Result<T, E>,
) -> Result<(), E> {
    while buffer.len() < max_messages as usize {
        buffer.push(receive().await?);
    }
    Ok(())
}

/// A deadline covers attempts, retry delays, recovery, and credit cleanup together.
/// A partial batch must be returned, not dropped behind an error after acquiring locks.
pub(super) async fn with_deadline<T, E>(
    mut buffer: Vec<T>,
    max_wait: Duration,
    operation: impl AsyncFnOnce(&mut Vec<T>) -> Result<(), E>,
) -> Result<Vec<T>, E> {
    match crate::util::time::timeout(max_wait, operation(&mut buffer)).await {
        Ok(Err(error)) if buffer.is_empty() => Err(error),
        _ => Ok(buffer),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{fill, with_deadline, CreditLease, Recovery, StopCredit};
    use std::{cell::Cell, rc::Rc};

    struct TestLink {
        active: Rc<Cell<bool>>,
        can_stop: bool,
    }
    impl StopCredit for TestLink {
        fn stop_credit(&mut self) -> bool {
            if self.can_stop {
                self.active.set(false);
            }
            self.can_stop
        }
    }
    impl Drop for TestLink {
        fn drop(&mut self) {
            self.active.set(false);
        }
    }

    #[tokio::test]
    async fn cancelled_partial_receive_revokes_credit_or_invalidates_the_link() {
        for can_stop in [true, false] {
            let active = Rc::new(Cell::new(true));
            let mut slot = Some(TestLink {
                active: active.clone(),
                can_stop,
            });
            let mut recovery = Recovery::default();
            let result = with_deadline(Vec::new(), Duration::from_millis(1), async |buffer| {
                let _lease = CreditLease {
                    slot: &mut slot,
                    recovery: &mut recovery,
                    manual: true,
                };
                buffer.push(42);
                std::future::pending::<Result<(), ()>>().await
            })
            .await;
            assert_eq!(result, Ok(vec![42]));
            assert!(!active.get(), "credit survived cancellation");
            assert_eq!(slot.is_some(), can_stop);
            assert_eq!(recovery.needed, !can_stop);
        }
    }

    #[tokio::test]
    async fn short_polls_keep_recovery_and_the_original_backoff_deadline() {
        let mut recovery = Recovery::default();
        recovery.failed(Some(Duration::from_millis(800)));
        let original_deadline = recovery.retry_at;
        for _ in 0..3 {
            let result = with_deadline(Vec::<u8>::new(), Duration::from_millis(1), async |_| {
                crate::util::time::sleep(recovery.remaining()).await;
                std::future::pending::<Result<(), &str>>().await
            })
            .await;
            assert_eq!(
                recovery.finish(result, "recovery pending"),
                Err("recovery pending")
            );
            assert_eq!(recovery.retry_at, original_deadline);
        }
        // Once the persistent deadline elapses, the next call must recover rather
        // than treating a broken link as an ordinary empty receive.
        recovery.retry_at = Some(crate::util::time::Instant::now());
        crate::util::time::sleep(recovery.remaining()).await;
        assert!(recovery.needed);
        recovery.recovered();
        assert_eq!(
            recovery.finish(Ok(vec![42]), "recovery pending"),
            Ok(vec![42])
        );
    }

    #[tokio::test]
    async fn retry_fills_only_remaining_slots() {
        let mut buffer = Vec::new();
        let mut first = [Ok(1), Err("broken link")].into_iter();
        assert_eq!(
            fill(&mut buffer, 3, async || first.next().unwrap()).await,
            Err("broken link")
        );
        let mut next = 1;
        fill(&mut buffer, 3, async || {
            next += 1;
            Ok::<_, &str>(next)
        })
        .await
        .unwrap();
        assert_eq!(buffer, [1, 2, 3]);
        assert_eq!(next, 3);
    }

    #[tokio::test]
    async fn errors_return_acquired_custody_but_empty_errors_remain_errors() {
        let partial = with_deadline(Vec::new(), Duration::from_secs(1), async |buffer| {
            buffer.push(42);
            Err("broken link")
        })
        .await;
        assert_eq!(partial, Ok(vec![42]));
        let empty = with_deadline(Vec::<u8>::new(), Duration::from_secs(1), async |_| {
            Err("broken link")
        })
        .await;
        assert_eq!(empty, Err("broken link"));
    }

    #[tokio::test]
    async fn deadline_returns_partial_or_empty_batches_without_waiting_for_cleanup() {
        for acquired in [false, true] {
            let result = with_deadline(Vec::new(), Duration::from_millis(10), async |buffer| {
                if acquired {
                    buffer.push(42);
                }
                // Models waiting for a transfer, retry delay, recovery or drain.
                std::future::pending::<Result<(), ()>>().await
            })
            .await;
            assert_eq!(result, Ok(if acquired { vec![42] } else { vec![] }));
        }
    }
}
