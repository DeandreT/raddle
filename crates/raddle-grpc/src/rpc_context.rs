use tokio::sync::watch;
use tokio::time::Instant;

/// A local RPC deadline and cancellation signal, supplied to the typed handler.
/// Cancellation does not undo completed effects and does not stop other RPCs.
#[derive(Clone, Debug)]
pub struct RpcContext {
    deadline: Instant,
    cancelled: watch::Sender<bool>,
}

impl RpcContext {
    pub(crate) fn new(deadline: Instant) -> Self {
        let (cancelled, _) = watch::channel(false);
        Self {
            deadline,
            cancelled,
        }
    }

    /// The monotonic local deadline, capped by server policy and grpc-timeout.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Whether the RPC's future has been cancelled or its deadline has elapsed.
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow() || Instant::now() >= self.deadline
    }
    /// Waits for cancellation or deadline without spawning a task.
    pub async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        tokio::select! {
            _ = receiver.wait_for(|cancelled| *cancelled) => {},
            _ = tokio::time::sleep_until(self.deadline) => {},
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.send_replace(true);
    }
}

pub(crate) struct CancelOnDrop {
    context: RpcContext,
    armed: bool,
}

impl CancelOnDrop {
    pub(crate) fn new(context: RpcContext) -> Self {
        Self {
            context,
            armed: true,
        }
    }
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.context.cancel();
        }
    }
}
