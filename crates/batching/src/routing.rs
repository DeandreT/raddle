use std::collections::BTreeSet;

/// Explicit registration combination semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Combination {
    /// Direct processing shadows batching, with a diagnostic (or startup error).
    Classic,
    /// Direct and every named batch consumer have independent custody/retry state.
    Separated,
}

/// Execution topology cannot silently claim mixed batch/unbatched sequencing.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionQueue {
    /// Dedicated independent queue, explicitly opting out of mixed sequencing.
    Independent(String),
}

/// Deterministic startup diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Diagnostic {
    /// Classic direct handling disabled the named batch consumer.
    Shadowed(String),
    /// Explicit dedicated queue does not sequence against unbatched queues.
    Unsequenced(String),
}

/// Frozen explicit routing decision. Use its destinations for canonical fan-out custody.
#[derive(Clone, Debug)]
pub struct RoutePlan {
    destinations: Vec<String>,
    diagnostics: Vec<Diagnostic>,
}

/// Startup assertions, rather than silent loss of a promised guarantee.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoutingError {
    /// Batch/direct conflict forbidden by the application.
    #[error("direct handling shadows batching in classic mode")]
    Conflict,
    /// This queue explicitly opts out of mixed sequencing.
    #[error("batch execution is not sequenced with unbatched queues")]
    Unsequenced,
    /// Invalid or duplicated destination.
    #[error("invalid or duplicated batch destination")]
    Destination,
}

impl RoutePlan {
    /// Validates explicit named consumers. Batch-only consumers use independent
    /// destinations in either mode; a classic direct consumer shadows them.
    pub fn build(
        mode: Combination,
        direct: Option<String>,
        batches: Vec<ExecutionQueue>,
        assert_no_conflicts: bool,
        assert_sequenced: bool,
    ) -> Result<Self, RoutingError> {
        let mut destinations = Vec::new();
        let mut diagnostics = Vec::new();
        let mut seen = BTreeSet::new();
        for name in direct
            .iter()
            .chain(batches.iter().map(|ExecutionQueue::Independent(name)| name))
        {
            if raddle::SubscriptionName::try_new(name).is_err() || !seen.insert(name) {
                return Err(RoutingError::Destination);
            }
        }
        let conflict = mode == Combination::Classic && direct.is_some() && !batches.is_empty();
        if conflict && assert_no_conflicts {
            return Err(RoutingError::Conflict);
        }
        if assert_sequenced && !batches.is_empty() && !conflict {
            return Err(RoutingError::Unsequenced);
        }
        if let Some(direct) = direct {
            destinations.push(direct);
        }
        for ExecutionQueue::Independent(name) in batches {
            if conflict {
                diagnostics.push(Diagnostic::Shadowed(name));
            } else {
                diagnostics.push(Diagnostic::Unsequenced(name.clone()));
                destinations.push(name);
            }
        }
        Ok(Self {
            destinations,
            diagnostics,
        })
    }
    /// Independent subscriptions to stage in a single ingress transaction.
    pub fn destinations(&self) -> &[String] {
        &self.destinations
    }
    /// Stable registration-order diagnostics to display during startup.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
}
