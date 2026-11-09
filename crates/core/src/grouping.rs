//! Optional native dispatch metadata, separate from SQL predecessor ordering.

use serde::{Deserialize, Serialize};

/// Logical group identity. Empty is a matched identity, not absence.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupId(String);

impl GroupId {
    /// Constructs a logical identity; the enclosing envelope enforces byte bounds.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// Exact logical value, including a possible empty string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GroupId(..)")
    }
}

/// Additive per-send overrides. These do not set database Delivery.partition_key.
#[derive(Clone, Default)]
pub struct DispatchOptions {
    /// Explicit logical identity, taking precedence over all grouping rules.
    pub group: Option<GroupId>,
    /// Explicit transport routing key, independent of logical identity.
    pub partition_key: Option<String>,
}

/// Versioned immutable native dispatch metadata. Not authenticated tenant state.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dispatch {
    version: u32,
    resolved: bool,
    group: Option<GroupId>,
    partition_key: Option<String>,
}

impl std::fmt::Debug for DispatchOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DispatchOptions(..)")
    }
}
impl std::fmt::Debug for Dispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatch")
            .field("resolved", &self.resolved)
            .finish_non_exhaustive()
    }
}

impl Dispatch {
    /// Explicit overrides before grouping resolution.
    pub fn explicit(options: DispatchOptions) -> Self {
        Self {
            version: 1,
            resolved: false,
            group: options.group,
            partition_key: options.partition_key,
        }
    }
    /// Frozen resolution, including a deliberate absence of logical group.
    pub fn resolved(group: Option<GroupId>, partition_key: Option<String>) -> Self {
        Self {
            version: 1,
            resolved: true,
            group,
            partition_key,
        }
    }
    /// Logical identity, if determined.
    pub fn group(&self) -> Option<&GroupId> {
        self.group.as_ref()
    }
    /// Explicit transport key, not a SQL predecessor key.
    pub fn partition_key(&self) -> Option<&str> {
        self.partition_key.as_deref()
    }
    /// Whether rules have already resolved this envelope.
    pub fn is_resolved(&self) -> bool {
        self.resolved
    }
    pub(crate) fn validate(&self) -> bool {
        self.version == 1
    }
}
