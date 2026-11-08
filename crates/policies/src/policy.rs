use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use crate::ErrorFilter;

/// Invalid policy configuration or exhausted arithmetic bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid policy configuration")]
pub struct PolicyErrorCode;

/// Handling and publication have independent attempt budgets and policies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    /// Execute an incoming handler.
    Handling,
    /// Publish and wait for transport confirmation.
    Sending,
}

/// Counters count actual executions, including the original attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptCounters {
    /// Handler invocations already begun.
    pub handling: u64,
    /// Publication calls already begun.
    pub sending: u64,
}

impl AttemptCounters {
    /// Advances only the selected direction; overflow is an explicit error.
    pub fn begin(&mut self, direction: Direction) -> Result<u64, PolicyErrorCode> {
        let count = match direction {
            Direction::Handling => &mut self.handling,
            Direction::Sending => &mut self.sending,
        };
        *count = count.checked_add(1).ok_or(PolicyErrorCode)?;
        Ok(*count)
    }
    /// Returns the selected attempt count.
    pub fn get(self, direction: Direction) -> u64 {
        match direction {
            Direction::Handling => self.handling,
            Direction::Sending => self.sending,
        }
    }
}

/// One recovery operation. Listener pause is an orthogonal `Step` decoration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Action {
    /// Retry within the current execution, only with explicit replay safety.
    Immediate,
    /// Interruptible in-execution delay before a safe retry.
    Cooldown(Duration),
    /// Give up this claim and append to the same endpoint's queue tail.
    Requeue,
    /// Release custody into future scheduled work, retaining identity.
    Schedule(Duration),
    /// Terminal successful settlement without a dead letter.
    Discard,
    /// Terminal quarantine, distinct from discard.
    DeadLetter,
}

/// A recovery action optionally pauses only this endpoint before transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Step {
    /// Queue or inline recovery operation.
    pub action: Action,
    /// Endpoint pause, independent of message due time.
    pub pause: Option<Duration>,
}
impl Step {
    /// Constructs an action with no listener pause.
    pub fn new(action: Action) -> Self {
        Self {
            action,
            pause: None,
        }
    }
    /// Adds an endpoint-local pause, including to an indefinite tail.
    pub fn pausing(mut self, duration: Duration) -> Self {
        self.pause = Some(duration);
        self
    }
}

/// Explicit behavior after all finite recovery slots have been consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tail {
    /// A terminal action, validated to discard or dead-letter.
    Terminal(Action),
    /// Repeat forever until success/cancellation; opt-in, never implicit.
    Repeat(Step),
}

/// A rule-wide additive jitter strategy; samples are supplied by the caller.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Jitter {
    /// Preserve base delay.
    #[default]
    None,
    /// Uniform range from base to twice base.
    Full,
    /// Add up to this percentage; valid values 1..=10000.
    BoundedPercent(u16),
    /// Add up to `base * 2 * attempt`.
    Exponential,
}
impl Jitter {
    /// Applies a sample in the full u64 domain. Never shortens base delay.
    /// Saturation is explicit at `Duration::MAX` for all overflow cases.
    pub fn delay(self, base: Duration, attempt: u64, sample: u64) -> Duration {
        let factor = match self {
            Self::None => 0,
            Self::Full => 100,
            Self::BoundedPercent(p) => u128::from(p),
            Self::Exponential => u128::from(attempt).saturating_mul(200),
        };
        let maximum = base.as_nanos().saturating_mul(factor) / 100;
        // Split the product so large delays do not collapse the sampled range
        // merely because maximum * sample overflows an intermediate u128.
        let scale = u128::from(u64::MAX);
        let sample = u128::from(sample);
        let extra = (maximum / scale) * sample + (maximum % scale) * sample / scale;
        let total = base.as_nanos().saturating_add(extra);
        let seconds = total / 1_000_000_000;
        if seconds > u128::from(u64::MAX) {
            Duration::MAX
        } else {
            Duration::new(seconds as u64, (total % 1_000_000_000) as u32)
        }
    }
}

/// Immutable validated finite slots plus an explicit terminal/indefinite tail.
#[derive(Clone)]
pub struct Plan {
    steps: Vec<Step>,
    tail: Tail,
    jitter: Jitter,
}
impl Plan {
    /// Each slot applies after one failed execution, so N slots permit N+1
    /// executions. Use no slots for a maximum of one original attempt.
    pub fn new(steps: Vec<Step>, tail: Tail, jitter: Jitter) -> Result<Self, PolicyErrorCode> {
        if steps.len() > 1000
            || matches!(jitter, Jitter::BoundedPercent(0 | 10001..))
            || matches!(tail, Tail::Terminal(action) if !matches!(action, Action::Discard | Action::DeadLetter))
        {
            return Err(PolicyErrorCode);
        }
        if matches!(tail, Tail::Repeat(step) if matches!(step.action, Action::Discard | Action::DeadLetter))
        {
            return Err(PolicyErrorCode);
        }
        if steps
            .iter()
            .any(|s| matches!(s.action, Action::Discard | Action::DeadLetter))
        {
            return Err(PolicyErrorCode);
        }
        Ok(Self {
            steps,
            tail,
            jitter,
        })
    }
    /// Returns the slot for a 1-based actual attempt; zero is invalid. The same
    /// sample independently scales each original action/pause delay once. A
    /// composite step never jitters an already-jittered duration or their sum.
    pub fn step(&self, attempt: u64, sample: u64) -> Result<Step, PolicyErrorCode> {
        let index = attempt.checked_sub(1).ok_or(PolicyErrorCode)?;
        let mut step = usize::try_from(index)
            .ok()
            .and_then(|i| self.steps.get(i))
            .copied()
            .unwrap_or_else(|| match self.tail {
                Tail::Terminal(action) => Step::new(action),
                Tail::Repeat(step) => step,
            });
        step.action = match step.action {
            Action::Cooldown(d) => Action::Cooldown(self.jitter.delay(d, attempt, sample)),
            Action::Schedule(d) => Action::Schedule(self.jitter.delay(d, attempt, sample)),
            action => action,
        };
        step.pause = step
            .pause
            .map(|delay| self.jitter.delay(delay, attempt, sample));
        Ok(step)
    }
}

/// First matching filter wins; rule order is never reordered by specificity.
#[derive(Clone)]
pub struct Rule {
    /// Error matching contract.
    pub filter: ErrorFilter,
    /// Recovery slots shared by all scopes that use this rule.
    pub plan: Plan,
}

/// Scoped policies frozen by ownership once given to an executor.
#[derive(Default, Clone)]
pub struct PolicySet {
    global: Vec<Rule>,
    endpoints: BTreeMap<String, Vec<Rule>>,
    messages: BTreeMap<String, Vec<Rule>>,
}
impl PolicySet {
    /// Appends a global fall-through rule.
    pub fn global(&mut self, rule: Rule) {
        self.global.push(rule);
    }
    /// Appends an endpoint rule; matching endpoint rules precede globals.
    pub fn endpoint(&mut self, endpoint: impl Into<String>, rule: Rule) {
        self.endpoints
            .entry(endpoint.into())
            .or_default()
            .push(rule);
    }
    /// Appends a message rule; handling checks these before endpoint/global.
    pub fn message(&mut self, contract: impl Into<String>, rule: Rule) {
        self.messages.entry(contract.into()).or_default().push(rule);
    }
    /// Sending ignores handler/message rules. A matched exhausted rule stays
    /// selected rather than falling through to a less specific rule.
    pub fn select(
        &self,
        direction: Direction,
        endpoint: &str,
        contract: &str,
        error: &(dyn Error + 'static),
    ) -> Option<&Plan> {
        let message = (direction == Direction::Handling)
            .then(|| self.messages.get(contract))
            .flatten();
        message
            .into_iter()
            .flatten()
            .chain(self.endpoints.get(endpoint).into_iter().flatten())
            .chain(&self.global)
            .find(|rule| rule.filter.matches(error))
            .map(|rule| &rule.plan)
    }
}
