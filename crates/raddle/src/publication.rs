use crate::{ExecutionError, SubscriptionName};

/// A named subscriber's independent execution result.
#[derive(Debug)]
pub struct SubscriptionOutcome<E> {
    pub(crate) name: SubscriptionName,
    pub(crate) result: Result<(), ExecutionError<E>>,
}

impl<E> SubscriptionOutcome<E> {
    /// Identifies the subscriber independently of its implementation type.
    pub fn name(&self) -> &SubscriptionName {
        &self.name
    }

    /// Borrows this subscriber's result.
    pub fn result(&self) -> Result<(), &ExecutionError<E>> {
        self.result.as_ref().copied()
    }

    /// Takes the result, preserving its concrete domain error.
    pub fn into_result(self) -> Result<(), ExecutionError<E>> {
        self.result
    }
}

/// All local subscriber outcomes in registration order.
///
/// Failures do not short-circuit siblings. An event with no subscriptions
/// succeeds with an empty report. Cancellation drops unfinished handlers and
/// produces no report; already-performed side effects cannot be rolled back.
#[derive(Debug)]
pub struct PublishReport<E> {
    pub(crate) outcomes: Vec<SubscriptionOutcome<E>>,
}

impl<E> PublishReport<E> {
    /// Iterates outcomes without allocating or transferring their ownership.
    pub fn outcomes(&self) -> impl ExactSizeIterator<Item = &SubscriptionOutcome<E>> {
        self.outcomes.iter()
    }

    /// Whether every registered subscriber completed successfully.
    pub fn is_success(&self) -> bool {
        self.outcomes.iter().all(|outcome| outcome.result.is_ok())
    }
}

impl<E> IntoIterator for PublishReport<E> {
    type Item = SubscriptionOutcome<E>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.outcomes.into_iter()
    }
}
