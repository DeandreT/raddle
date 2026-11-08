use std::any::{self, TypeId};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::{Assembler, Options};

/// Pending originals for one Rust message type. Names are diagnostic, not wire contracts.
#[derive(Debug, PartialEq, Eq)]
pub struct TypePending {
    /// Rust diagnostic type name.
    pub name: &'static str,
    /// Originals still held by accumulation, execution or retry tokens.
    pub pending: usize,
}

/// Payload-free snapshot across explicitly registered assemblers.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PendingSnapshot {
    /// Every registered original, including in-process admissions.
    pub total: usize,
    /// Counts grouped by actual Rust type identity, not payload string metadata.
    pub by_type: Vec<TypePending>,
    /// Listener-origin originals only. In-process assemblers are excluded.
    pub by_listener: BTreeMap<String, usize>,
}

struct Registration {
    type_id: TypeId,
    type_name: &'static str,
    listener: Option<String>,
    pending: Weak<AtomicUsize>,
}

/// Optional shared observation only; owns no message, task or execution lifecycle.
#[derive(Clone, Default)]
pub struct Registry {
    registrations: Arc<Mutex<Vec<Registration>>>,
}

impl Registry {
    /// Registers an assembler. None marks in-process origin, not a listener.
    /// Outstanding batch/expiry tokens keep their registration observable even
    /// when the original assembler has been dropped.
    pub fn assembler<T: 'static>(
        &self,
        listener: Option<&str>,
        options: Options,
    ) -> Result<Assembler<T>, &'static str> {
        if let Some(listener) = listener {
            raddle::SubscriptionName::try_new(listener).map_err(|_| "invalid listener")?;
        }
        let assembler = Assembler::new(options);
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| "observation lock poisoned")?;
        registrations.retain(|registration| registration.pending.strong_count() > 0);
        registrations.push(Registration {
            type_id: TypeId::of::<T>(),
            type_name: any::type_name::<T>(),
            listener: listener.map(str::to_owned),
            pending: assembler.counter(),
        });
        Ok(assembler)
    }
    /// Samples pending counters; concurrent admissions may advance after sampling.
    /// Successful handler return does not decrement counts: the caller must retain
    /// its owned batch through actual settlement.
    pub fn snapshot(&self) -> Result<PendingSnapshot, &'static str> {
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| "observation lock poisoned")?;
        registrations.retain(|registration| registration.pending.strong_count() > 0);
        let mut snapshot = PendingSnapshot::default();
        let mut by_type: BTreeMap<TypeId, TypePending> = BTreeMap::new();
        for registration in registrations.iter() {
            let Some(counter) = registration.pending.upgrade() else {
                continue;
            };
            let pending = counter.load(Ordering::Acquire);
            snapshot.total += pending;
            by_type
                .entry(registration.type_id)
                .or_insert(TypePending {
                    name: registration.type_name,
                    pending: 0,
                })
                .pending += pending;
            if let Some(listener) = &registration.listener {
                *snapshot.by_listener.entry(listener.clone()).or_default() += pending;
            }
        }
        snapshot.by_type = by_type.into_values().collect();
        snapshot.by_type.sort_by_key(|value| value.name);
        Ok(snapshot)
    }
}
