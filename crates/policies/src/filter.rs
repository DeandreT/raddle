use std::error::Error;
use std::sync::Arc;

/// Owned handler/send error. Display text is never automatically persisted.
pub type PolicyError = Box<dyn Error + Send + Sync + 'static>;
type Predicate = Arc<dyn Fn(&(dyn Error + 'static)) -> bool + Send + Sync>;

/// Ordered rules compose typed, predicate, cause and Boolean error filters.
#[derive(Clone)]
pub struct ErrorFilter(Predicate);

impl ErrorFilter {
    /// Matches every error, including a terminal cause.
    pub fn any() -> Self {
        Self(Arc::new(|_| true))
    }

    /// Matches the exact Rust error type, not its Display representation.
    pub fn typed<E: Error + 'static>() -> Self {
        Self::predicate(|error| error.is::<E>())
    }

    /// Matches application-defined error properties. Predicates must be pure.
    pub fn predicate(f: impl Fn(&(dyn Error + 'static)) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Matches any source, excluding the outer error, stopping at depth 32.
    /// A cyclic `Error::source` implementation cannot hang the policy engine.
    pub fn cause(self) -> Self {
        Self::predicate(move |error| {
            let mut source = error.source();
            for _ in 0..32 {
                let Some(error) = source else {
                    return false;
                };
                if self.matches(error) {
                    return true;
                }
                source = error.source();
            }
            false
        })
    }

    /// Logical intersection; both predicates examine the same outer error.
    pub fn and(self, other: Self) -> Self {
        Self::predicate(move |error| self.matches(error) && other.matches(error))
    }

    /// Logical union with short-circuit evaluation.
    pub fn or(self, other: Self) -> Self {
        Self::predicate(move |error| self.matches(error) || other.matches(error))
    }

    /// Inverts this filter.
    pub fn negate(self) -> Self {
        Self::predicate(move |error| !self.matches(error))
    }

    /// Evaluates without formatting or taking ownership of the error.
    pub fn matches(&self, error: &(dyn Error + 'static)) -> bool {
        (self.0)(error)
    }
}
