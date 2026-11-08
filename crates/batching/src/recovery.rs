use std::collections::BTreeMap;

use uuid::Uuid;

use crate::Member;

/// Explicit original-member outcome. Retry is never acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decision {
    /// Business effects for this original have committed in the settlement transaction.
    Commit,
    /// Persist terminal quarantine using a bounded application-owned code.
    Poison(String),
    /// Preserve custody for corrected/reduced retry, excluding committed survivors.
    Retry,
}

/// A complete validated disposition over original identities, not input positions.
#[derive(Clone, Debug)]
pub struct Resolution {
    decisions: BTreeMap<Uuid, Decision>,
}

/// Invalid plans fail before any settlement is attempted.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResolutionError {
    /// Named member is not part of this execution.
    #[error("unknown original member")]
    Unknown,
    /// The same identity was named more than once.
    #[error("duplicate original member")]
    Duplicate,
    /// A quarantine code is empty or exceeds 128 bytes.
    #[error("invalid quarantine code")]
    Code,
    /// Custody participants with the same coalescing key disagree.
    #[error("coalesced originals must have the same disposition")]
    Coalescing,
}

impl Resolution {
    /// Resolves named originals and leaves all unnamed originals retryable.
    /// Naming a survivor expands its decision to ALL originals of its coalesced key.
    pub fn named<T>(
        members: &[Member<T>],
        named: impl IntoIterator<Item = (Uuid, Decision)>,
    ) -> Result<Self, ResolutionError> {
        let mut explicit = BTreeMap::new();
        for (id, decision) in named {
            if !members
                .iter()
                .any(|member| member.context.delivery_id == id)
            {
                return Err(ResolutionError::Unknown);
            }
            if let Decision::Poison(code) = &decision
                && (code.is_empty() || code.len() > 128)
            {
                return Err(ResolutionError::Code);
            }
            if explicit.insert(id, decision).is_some() {
                return Err(ResolutionError::Duplicate);
            }
        }
        let mut decisions = BTreeMap::new();
        for member in members {
            let mut decision = explicit.get(&member.context.delivery_id);
            if let Some(key) = &member.coalesce {
                for other in members
                    .iter()
                    .filter(|other| other.coalesce.as_ref() == Some(key))
                {
                    if let Some(named) = explicit.get(&other.context.delivery_id) {
                        if decision.is_some_and(|decision| decision != named) {
                            return Err(ResolutionError::Coalescing);
                        }
                        decision = Some(named);
                    }
                }
            }
            decisions.insert(
                member.context.delivery_id,
                decision.cloned().unwrap_or(Decision::Retry),
            );
        }
        Ok(Self { decisions })
    }
    /// A successful batch settles every original, not only input survivors.
    pub fn committed<T>(members: &[Member<T>]) -> Self {
        Self {
            decisions: members
                .iter()
                .map(|member| (member.context.delivery_id, Decision::Commit))
                .collect(),
        }
    }
    /// All original decisions. Identifies precisely which successful state must not replay.
    pub fn decisions(&self) -> &BTreeMap<Uuid, Decision> {
        &self.decisions
    }
}
