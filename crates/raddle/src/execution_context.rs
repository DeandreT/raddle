use uuid::Uuid;

/// Local execution identity, propagated explicitly through middleware and cascades.
///
/// These values are diagnostics, not authentication or trusted wire metadata.
/// Context does not own tasks or cancellation: dropping the caller's future
/// cancels all unfinished local work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    execution_id: Uuid,
    correlation_id: Uuid,
    parent_id: Option<Uuid>,
    depth: usize,
}

impl ExecutionContext {
    /// Creates a root execution with a fresh correlation and execution identity.
    pub fn new() -> Self {
        let execution_id = Uuid::new_v4();
        Self {
            execution_id,
            correlation_id: execution_id,
            parent_id: None,
            depth: 0,
        }
    }

    /// Identity of this execution, independent of message wire identity.
    pub fn execution_id(&self) -> Uuid {
        self.execution_id
    }

    /// Identity shared by the root and all its cascading effects.
    pub fn correlation_id(&self) -> Uuid {
        self.correlation_id
    }

    /// Parent execution, absent for a root.
    pub fn parent_id(&self) -> Option<Uuid> {
        self.parent_id
    }

    /// Number of cascading edges from the root.
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub(crate) fn child(self) -> Self {
        Self {
            execution_id: Uuid::new_v4(),
            correlation_id: self.correlation_id,
            parent_id: Some(self.execution_id),
            depth: self.depth.saturating_add(1),
        }
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}
