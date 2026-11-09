use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use raddle::wire::Envelope;

use super::config::Source;
use super::projection::Projection;
use super::{Key, LocalRouting, RoutingConfig, RoutingError, SourceOptions};
use crate::TrustedOrigin;

/// One effective native delivery, including its typed projection path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainedRoute {
    /// Configured symbolic destination, never an inbound URI.
    pub endpoint: String,
    /// Final concrete contract name.
    pub contract: String,
    /// Final concrete schema version.
    pub version: u32,
    /// Stable versioned projection IDs, in application order.
    pub projections: Vec<String>,
    /// Every contributing source for this deduplicated delivery.
    pub sources: Vec<String>,
}

/// Frozen source evaluation; skipped steps are explicit rather than inferred.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingStep {
    /// Stable source or built-in stage ID.
    pub source: String,
    /// Bounded application-readable explanation, without payload data.
    pub description: String,
    /// Whether an earlier stage prevented consultation.
    pub skipped: bool,
    /// Source continuation and contribution behavior.
    pub options: SourceOptions,
    /// Number of contributions before delivery deduplication.
    pub contributions: usize,
    /// Accumulated non-pass-through contributions after this stage.
    pub blocking: usize,
}

/// The same frozen plan consumed by actual publication execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingExplanation {
    /// Original concrete contract name.
    pub contract: String,
    /// Original concrete schema version.
    pub version: u32,
    /// Ordered custom and built-in evaluations.
    pub steps: Vec<RoutingStep>,
    /// Effective deduplicated deliveries.
    pub routes: Vec<ExplainedRoute>,
    /// Exact legacy RPC aliases; publication projections never rewrite this view.
    pub invocation: Vec<String>,
    /// Readable descriptions for source IDs, including selector rules and projections.
    pub sources: BTreeMap<String, String>,
}

#[derive(Clone)]
pub(crate) struct Route {
    pub destination: String,
    pub path: Vec<Arc<Projection>>,
    sources: Vec<String>,
    output: Key,
}
impl Route {
    pub fn direct(destination: String, key: Key, source: &str) -> Self {
        Self {
            destination,
            output: key,
            path: Vec::new(),
            sources: vec![source.into()],
        }
    }
    fn same_delivery(&self, other: &Self) -> bool {
        self.destination == other.destination
            && self
                .path
                .iter()
                .map(|p| (&p.id, p.version))
                .eq(other.path.iter().map(|p| (&p.id, p.version)))
    }
    fn describe(&self) -> ExplainedRoute {
        ExplainedRoute {
            endpoint: self.destination.clone(),
            contract: self.output.0.clone(),
            version: self.output.1,
            projections: self
                .path
                .iter()
                .map(|p| format!("{}@{}", p.id, p.version))
                .collect(),
            sources: self.sources.clone(),
        }
    }
    pub fn prepare(
        &self,
        root: &Envelope,
        origin: &TrustedOrigin,
        headers: &BTreeSet<String>,
    ) -> Result<Envelope, RoutingError> {
        let mut envelope = root.clone();
        for (position, projection) in self.path.iter().enumerate() {
            envelope =
                projection.apply(&envelope, root, origin, &self.path[..=position], headers)?;
        }
        Ok(envelope)
    }
    pub fn path_key(&self) -> Vec<(String, u32)> {
        self.path
            .iter()
            .map(|p| (p.id.clone(), p.version))
            .collect()
    }
}

#[derive(Clone)]
struct Plan {
    routes: Vec<Route>,
    explanation: RoutingExplanation,
}

/// Publication and operation-specific legacy invocation views share this snapshot.
#[derive(Clone, Default)]
pub(crate) struct Resolver {
    plans: BTreeMap<Key, Plan>,
    pub targets: BTreeMap<Key, BTreeSet<String>>,
}
impl Resolver {
    pub fn legacy(routes: &BTreeMap<Key, Vec<String>>) -> Self {
        let mut resolver = Self::default();
        for (key, aliases) in routes {
            let routes: Vec<_> = aliases
                .iter()
                .map(|alias| Route::direct(alias.clone(), key.clone(), "legacy-explicit"))
                .collect();
            let explanation = RoutingExplanation {
                contract: key.0.clone(),
                version: key.1,
                steps: vec![RoutingStep {
                    source: "legacy-explicit".into(),
                    description: "Exact routes, including legacy-pinned handler bindings".into(),
                    skipped: false,
                    options: SourceOptions::TERMINATING,
                    contributions: routes.len(),
                    blocking: routes.len(),
                }],
                routes: routes.iter().map(Route::describe).collect(),
                invocation: aliases.clone(),
                sources: BTreeMap::from([(
                    "legacy-explicit".into(),
                    "Exact routes, including legacy-pinned handler bindings".into(),
                )]),
            };
            resolver
                .targets
                .insert(key.clone(), aliases.iter().cloned().collect());
            resolver.plans.insert(
                key.clone(),
                Plan {
                    routes,
                    explanation,
                },
            );
        }
        resolver
    }
    pub fn compile(
        config: &RoutingConfig,
        legacy: &BTreeMap<Key, Vec<String>>,
        endpoints: &BTreeSet<String>,
    ) -> Result<Self, RoutingError> {
        let mut compiler = Compiler {
            config,
            legacy,
            raw: BTreeMap::new(),
            resolver: Self::legacy(legacy),
        };
        let all: BTreeSet<_> = config
            .messages
            .keys()
            .chain(legacy.keys())
            .cloned()
            .collect();
        if all.len() > 1024 {
            return Err(RoutingError::Invalid("contract capacity exceeded"));
        }
        for key in config.messages.keys() {
            compiler.check_graph(key, &mut Vec::new())?;
            let mut stages = Vec::new();
            for source in config
                .custom
                .iter()
                .chain(&config.explicit)
                .chain(&config.conventions)
            {
                let aliases = catch_unwind(AssertUnwindSafe(|| {
                    source.source.routes(&config.messages[key])
                }))
                .map_err(|_| RoutingError::Callback)??;
                if aliases.len() > 16 {
                    return Err(RoutingError::Invalid("source fanout exceeded"));
                }
                stages.push(
                    aliases
                        .iter()
                        .map(|alias| Route::direct(alias.as_str().into(), key.clone(), &source.id))
                        .collect::<Vec<_>>(),
                );
            }
            let mut targets: BTreeSet<_> = stages
                .iter()
                .flatten()
                .map(|route| route.destination.clone())
                .collect();
            targets.extend(legacy.get(key).into_iter().flatten().cloned());
            targets.extend(config.locals.get(key).into_iter().flatten().cloned());
            compiler.resolver.targets.insert(key.clone(), targets);
            compiler.raw.insert(key.clone(), stages);
        }
        for targets in compiler.resolver.targets.values() {
            if targets.iter().any(|target| !endpoints.contains(target)) {
                return Err(RoutingError::Invalid("source returned an unknown endpoint"));
            }
        }
        // Replace legacy plans for declared contracts before recursive compilation.
        for key in config.messages.keys() {
            compiler.resolver.plans.remove(key);
        }
        for key in config.messages.keys() {
            compiler.plan(key)?;
        }
        Ok(compiler.resolver)
    }
    pub fn routes(&self, key: &Key) -> &[Route] {
        self.plans.get(key).map_or(&[], |plan| &plan.routes)
    }
    pub fn invocation(&self, key: &Key) -> &[String] {
        self.plans
            .get(key)
            .map_or(&[], |plan| &plan.explanation.invocation)
    }
    pub fn explain(&self, key: &Key) -> RoutingExplanation {
        self.plans
            .get(key)
            .map(|plan| plan.explanation.clone())
            .unwrap_or_else(|| RoutingExplanation {
                contract: key.0.clone(),
                version: key.1,
                steps: Vec::new(),
                routes: Vec::new(),
                invocation: Vec::new(),
                sources: BTreeMap::new(),
            })
    }
}

struct Compiler<'a> {
    config: &'a RoutingConfig,
    legacy: &'a BTreeMap<Key, Vec<String>>,
    raw: BTreeMap<Key, Vec<Vec<Route>>>,
    resolver: Resolver,
}
impl Compiler<'_> {
    fn check_graph(&self, key: &Key, stack: &mut Vec<Key>) -> Result<(), RoutingError> {
        if stack.len() >= 16 || stack.contains(key) {
            return Err(RoutingError::Invalid("projection cycle or depth exceeded"));
        }
        stack.push(key.clone());
        for projection in self
            .config
            .projections
            .iter()
            .filter(|projection| &projection.from == key)
        {
            self.check_graph(&projection.to, stack)?;
        }
        stack.pop();
        Ok(())
    }
    fn plan(&mut self, key: &Key) -> Result<Plan, RoutingError> {
        if let Some(plan) = self.resolver.plans.get(key) {
            return Ok(plan.clone());
        }
        let mut evaluation = Evaluation::default();
        let raw = self.raw.get(key).cloned().unwrap_or_default();
        for (position, source) in self.config.custom.iter().enumerate() {
            evaluation.source(source, &raw[position])?;
        }
        let mut transformed = Vec::new();
        // Graph validity is checked independently, even for suppressed projections.
        if !evaluation.stopped {
            for projection in self
                .config
                .projections
                .iter()
                .filter(|projection| &projection.from == key)
                .cloned()
                .collect::<Vec<_>>()
            {
                let target = self.plan(&projection.to)?;
                for mut route in target.routes {
                    route.path.insert(0, Arc::clone(&projection));
                    route.sources.insert(0, projection.id.clone());
                    transformed.push(route);
                }
            }
        }
        evaluation.stage(
            "transforms",
            "Typed plaintext publication projections",
            SourceOptions::TERMINATING,
            &transformed,
        )?;
        let mut explicit: Vec<_> = self
            .legacy
            .get(key)
            .into_iter()
            .flatten()
            .map(|alias| Route::direct(alias.clone(), key.clone(), "legacy-explicit"))
            .collect();
        let start = self.config.custom.len();
        for routes in raw.iter().skip(start).take(self.config.explicit.len()) {
            explicit.extend(routes.clone());
        }
        evaluation.stage(
            "explicit",
            "Exact legacy bindings and declared selector rules",
            SourceOptions::TERMINATING,
            &explicit,
        )?;
        let local: Vec<_> = if self.config.local == LocalRouting::Disabled {
            Vec::new()
        } else {
            self.config
                .locals
                .get(key)
                .into_iter()
                .flatten()
                .map(|alias| Route::direct(alias.clone(), key.clone(), "local"))
                .collect()
        };
        evaluation.stage(
            "local",
            match self.config.local {
                LocalRouting::Disabled => "Conventional local routing disabled",
                LocalRouting::Additive => "Local routing continues into conventions",
                _ => "Conventional local receiving bindings",
            },
            SourceOptions {
                additive: self.config.local != LocalRouting::Terminating,
                pass_through: false,
            },
            &local,
        )?;
        let start = start + self.config.explicit.len();
        for (position, source) in self.config.conventions.iter().enumerate() {
            evaluation.source(source, &raw[start + position])?;
        }
        let explanation = RoutingExplanation {
            contract: key.0.clone(),
            version: key.1,
            steps: evaluation.steps,
            routes: evaluation.routes.iter().map(Route::describe).collect(),
            invocation: self.legacy.get(key).cloned().unwrap_or_default(),
            sources: self
                .config
                .custom
                .iter()
                .chain(&self.config.explicit)
                .chain(&self.config.conventions)
                .map(|source| (source.id.clone(), source.description.clone()))
                .chain(self.config.projections.iter().map(|projection| {
                    (
                        projection.id.clone(),
                        format!(
                            "Plaintext publication projection {}@{}: {} v{} to {} v{}",
                            projection.id,
                            projection.version,
                            projection.from.0,
                            projection.from.1,
                            projection.to.0,
                            projection.to.1
                        ),
                    )
                }))
                .chain([
                    (
                        "legacy-explicit".into(),
                        "Exact routes, including legacy-pinned handler bindings".into(),
                    ),
                    (
                        "local".into(),
                        "Conventional local receiving bindings".into(),
                    ),
                ])
                .collect(),
        };
        let plan = Plan {
            routes: evaluation.routes,
            explanation,
        };
        self.resolver.plans.insert(key.clone(), plan.clone());
        Ok(plan)
    }
}

#[derive(Default)]
struct Evaluation {
    stopped: bool,
    blocking: usize,
    routes: Vec<Route>,
    steps: Vec<RoutingStep>,
}
impl Evaluation {
    fn source(&mut self, source: &Source, routes: &[Route]) -> Result<(), RoutingError> {
        self.stage(&source.id, &source.description, source.options, routes)
    }
    fn stage(
        &mut self,
        id: &str,
        description: &str,
        options: SourceOptions,
        routes: &[Route],
    ) -> Result<(), RoutingError> {
        let skipped = self.stopped;
        if !skipped {
            // Count before deduplication: a duplicate normal contribution still blocks.
            if !options.pass_through {
                self.blocking += routes.len();
            }
            for route in routes {
                if let Some(existing) = self
                    .routes
                    .iter_mut()
                    .find(|existing| existing.same_delivery(route))
                {
                    for source in &route.sources {
                        if !existing.sources.contains(source) {
                            existing.sources.push(source.clone());
                        }
                    }
                } else {
                    if self.routes.len() >= 16 {
                        return Err(RoutingError::Invalid("effective fanout exceeded"));
                    }
                    self.routes.push(route.clone());
                }
            }
            self.stopped = !options.additive && self.blocking != 0;
        }
        self.steps.push(RoutingStep {
            source: id.into(),
            description: description.into(),
            skipped,
            options,
            contributions: if skipped { 0 } else { routes.len() },
            blocking: self.blocking,
        });
        Ok(())
    }
}
