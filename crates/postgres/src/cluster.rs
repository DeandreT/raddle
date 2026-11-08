//! Database-clock membership and fenced, named singleton agents.
//!
//! This module owns no background tasks. Applications explicitly register each
//! process incarnation, heartbeat it, and release assignments on shutdown.
//! [`Scope::enforce_agent`] protects database work in that same transaction; it
//! cannot fence an HTTP request, a file write, or another external side effect.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::Scope;

/// The reserved singleton used for leadership, not a separate election protocol.
pub const LEADER_AGENT: &str = "_leader";

/// A cluster operation failed. Database errors retain their SQLSTATE.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClusterError {
    /// Input exceeded a bound or used an unapproved name or role.
    #[error("invalid cluster input: {0}")]
    Invalid(&'static str),
    /// Another process configured this namespace differently.
    #[error("cluster namespace configuration differs")]
    ConfigurationConflict,
    /// The namespace has no capacity for another live member.
    #[error("cluster member capacity exhausted")]
    Capacity,
    /// A node is expired, unknown, or belongs to another namespace.
    #[error("cluster member is not live")]
    DeadMember,
    /// The lease is expired, released, or replaced by a newer epoch.
    #[error("stale cluster lease")]
    StaleLease,
    /// The caller-side operation deadline elapsed, including pool acquisition.
    #[error("cluster operation timed out; reconcile mutations before retrying")]
    Timeout,
    /// A PostgreSQL operation failed, including server-side query/lock deadlines.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// A configured named singleton and the role allowed to acquire it.
#[derive(Clone, Debug)]
pub struct AgentSpec {
    name: String,
    role: String,
}

impl AgentSpec {
    /// Defines an application agent using bounded lowercase ASCII identifiers.
    pub fn new(name: &str, required_role: &str) -> Result<Self, ClusterError> {
        identifier(name)?;
        Self::build(name, required_role)
    }

    /// Defines the reserved leadership agent with an explicit eligible role.
    pub fn leader(required_role: &str) -> Result<Self, ClusterError> {
        Self::build(LEADER_AGENT, required_role)
    }

    fn build(name: &str, role: &str) -> Result<Self, ClusterError> {
        identifier(role)?;
        Ok(Self {
            name: name.into(),
            role: role.into(),
        })
    }
}

/// Validated namespace policy, shared by every process using that namespace.
#[derive(Clone, Debug)]
pub struct ClusterOptions {
    namespace: String,
    roles: BTreeSet<String>,
    agents: BTreeMap<String, String>,
    max_members: u32,
    member_ttl: Duration,
    lease_ttl: Duration,
    operation_timeout: Duration,
}

impl ClusterOptions {
    /// Allows 1–32 roles and 1–256 agents, with a default budget of 128 members.
    /// Identifiers are 1–64 lowercase ASCII letters, digits, dots, dashes or
    /// underscores, starting with a letter. Unknown roles are never admitted.
    pub fn new(
        namespace: &str,
        roles: &[&str],
        agents: Vec<AgentSpec>,
    ) -> Result<Self, ClusterError> {
        identifier(namespace)?;
        if !(1..=32).contains(&roles.len()) || !(1..=256).contains(&agents.len()) {
            return Err(ClusterError::Invalid("role or agent budget"));
        }
        let mut allowed = BTreeSet::new();
        for role in roles {
            identifier(role)?;
            if !allowed.insert((*role).to_owned()) {
                return Err(ClusterError::Invalid("duplicate role"));
            }
        }
        let mut definitions = BTreeMap::new();
        for spec in agents {
            if !allowed.contains(&spec.role) || definitions.insert(spec.name, spec.role).is_some() {
                return Err(ClusterError::Invalid("unknown role or duplicate agent"));
            }
        }
        Ok(Self {
            namespace: namespace.into(),
            roles: allowed,
            agents: definitions,
            max_members: 128,
            member_ttl: Duration::from_secs(60),
            lease_ttl: Duration::from_secs(15),
            operation_timeout: Duration::from_secs(5),
        })
    }

    /// Sets the live-node budget (1–1024). Expired rows are reclaimed on registration.
    pub fn with_max_members(mut self, maximum: u32) -> Result<Self, ClusterError> {
        if !(1..=1024).contains(&maximum) {
            return Err(ClusterError::Invalid("member budget"));
        }
        self.max_members = maximum;
        Ok(self)
    }

    /// Sets membership and assignment TTLs (1 ms–2 h), and operation deadline
    /// (1 ms–30 s). Submillisecond values are rejected, not silently rounded.
    /// Each process may choose its own operation timeout; TTLs are shared policy.
    pub fn with_timing(
        mut self,
        membership: Duration,
        lease: Duration,
        timeout: Duration,
    ) -> Result<Self, ClusterError> {
        milliseconds(membership, 7_200_000)?;
        milliseconds(lease, 7_200_000)?;
        milliseconds(timeout, 30_000)?;
        self.member_ttl = membership;
        self.lease_ttl = lease;
        self.operation_timeout = timeout;
        Ok(self)
    }

    fn configuration(&self) -> Value {
        serde_json::json!({
            "roles": self.roles, "agents": self.agents, "max_members": self.max_members,
            "member_ms": self.member_ttl.as_millis(), "lease_ms": self.lease_ttl.as_millis(),
        })
    }
}

/// A registered process incarnation. Private fields prevent fabricating an owner.
/// Cloning retains the same incarnation; a restart must call `register` again.
#[derive(Clone, Debug)]
pub struct Node {
    namespace: String,
    instance: Uuid,
}

impl Node {
    /// The random UUID generated for this particular registration.
    pub fn instance(&self) -> Uuid {
        self.instance
    }
}

/// An issued fencing capability. Not deserializable or constructible from strings.
#[derive(Clone, Debug)]
pub struct Lease {
    namespace: String,
    agent: String,
    owner: Uuid,
    epoch: i64,
}

impl Lease {
    /// The named singleton, including `_leader` for leadership.
    pub fn agent(&self) -> &str {
        &self.agent
    }
    /// Owning registration UUID; not a stable machine identity.
    pub fn owner(&self) -> Uuid {
        self.owner
    }
    /// A strictly increasing fence, retained across release and reacquisition.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }
}

/// Payload-free membership diagnostics, not an ownership capability.
#[derive(Debug)]
pub struct Member {
    /// Process incarnation.
    pub instance: Uuid,
    /// Application-provided display label (not an identifier).
    pub label: String,
    /// Validated, allowlisted roles.
    pub roles: Vec<String>,
    /// Last heartbeat according to the database clock.
    pub heartbeat_at: DateTime<Utc>,
    /// Membership deadline according to the database clock.
    pub expires_at: DateTime<Utc>,
}

/// Payload-free assignment diagnostics, never accepted as a fencing capability.
#[derive(Debug)]
pub struct Assignment {
    /// Named singleton.
    pub agent: String,
    /// Required eligible role.
    pub required_role: String,
    /// Last owner, possibly expired; `live` determines current assignment status.
    pub owner: Option<Uuid>,
    /// Last issued epoch, including released tombstones.
    pub epoch: i64,
    /// Last issued database-clock lease deadline.
    pub lease_until: Option<DateTime<Utc>>,
    /// Whether both owner membership and assignment remain live.
    pub live: bool,
}

/// Explicit PostgreSQL cluster coordinator. Cloning or dropping it changes no
/// membership, assignment, cancellation, or application task state.
#[derive(Clone)]
pub struct Cluster {
    pool: PgPool,
    options: ClusterOptions,
}

impl Cluster {
    /// Initializes a namespace after the application's explicit migrations.
    /// Conflicting role, agent, budget or TTL policy is rejected, not overwritten.
    pub async fn connect(pool: PgPool, options: ClusterOptions) -> Result<Self, ClusterError> {
        let cluster = Self { pool, options };
        cluster.bounded(async {
            let mut tx = cluster.pool.begin().await?;
            cluster.deadlines(&mut tx).await?;
            sqlx::query("INSERT INTO raddle_cluster_namespaces(namespace,configuration) VALUES($1,$2) ON CONFLICT DO NOTHING")
                .bind(&cluster.options.namespace).bind(cluster.options.configuration()).execute(&mut *tx).await?;
            cluster.lock(&mut tx, false).await?;
            for (name, role) in &cluster.options.agents {
                sqlx::query("INSERT INTO raddle_cluster_agents(namespace,name,required_role) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                    .bind(&cluster.options.namespace).bind(name).bind(role).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(())
        }).await?;
        Ok(cluster)
    }

    /// Registers a new UUID incarnation with a 1–128-byte, non-control label and
    /// at least one allowlisted role. Never restores an old registration.
    pub async fn register(&self, label: &str, roles: &[&str]) -> Result<Node, ClusterError> {
        if label.is_empty()
            || label.len() > 128
            || label.chars().any(char::is_control)
            || !(1..=32).contains(&roles.len())
        {
            return Err(ClusterError::Invalid("node label or roles"));
        }
        if roles.iter().any(|role| !self.options.roles.contains(*role)) {
            return Err(ClusterError::Invalid("unapproved role"));
        }
        let roles: BTreeSet<_> = roles.iter().map(|role| (*role).to_owned()).collect();
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            sqlx::query("DELETE FROM raddle_cluster_members WHERE namespace=$1 AND expires_at<=clock_timestamp()")
                .bind(&self.options.namespace).execute(&mut *tx).await?;
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_cluster_members WHERE namespace=$1")
                .bind(&self.options.namespace).fetch_one(&mut *tx).await?;
            if count >= i64::from(self.options.max_members) { return Err(ClusterError::Capacity); }
            let node = Node { namespace: self.options.namespace.clone(), instance: Uuid::new_v4() };
            sqlx::query("WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now) INSERT INTO raddle_cluster_members(namespace,instance,label,roles,registered_at,heartbeat_at,expires_at) SELECT $1,$2,$3,$4,instant.now,instant.now,instant.now+$5::bigint*interval '1 millisecond' FROM instant")
                .bind(&node.namespace).bind(node.instance).bind(label).bind(roles.into_iter().collect::<Vec<_>>())
                .bind(milliseconds(self.options.member_ttl, 7_200_000)?).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(node)
        }).await
    }

    /// Extends only a still-live incarnation; a heartbeat after expiry cannot revive it.
    pub async fn heartbeat(&self, node: &Node) -> Result<(), ClusterError> {
        self.node_namespace(node)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            let changed = sqlx::query("WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now) UPDATE raddle_cluster_members SET heartbeat_at=instant.now,expires_at=instant.now+$3::bigint*interval '1 millisecond' FROM instant WHERE namespace=$1 AND instance=$2 AND expires_at>instant.now")
                .bind(&node.namespace).bind(node.instance).bind(milliseconds(self.options.member_ttl, 7_200_000)?).execute(&mut *tx).await?;
            if changed.rows_affected()!=1 { return Err(ClusterError::DeadMember); }
            tx.commit().await?;
            Ok(())
        }).await
    }

    /// Acquires a configured singleton or returns `None` while another assignment
    /// is live. Every successful acquisition increments its retained epoch.
    /// A dead member cannot acquire even a currently unassigned agent.
    pub async fn acquire(&self, node: &Node, agent: &str) -> Result<Option<Lease>, ClusterError> {
        self.node_namespace(node)?;
        let role = self
            .options
            .agents
            .get(agent)
            .ok_or(ClusterError::Invalid("unconfigured agent"))?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            let roles = self.live_member(&mut tx, node.instance).await?;
            if !roles.contains(role) { return Err(ClusterError::Invalid("member lacks agent role")); }
            let epoch: Option<i64> = sqlx::query_scalar("UPDATE raddle_cluster_agents a SET owner=$3,epoch=epoch+1,lease_until=clock_timestamp()+$4::bigint*interval '1 millisecond' WHERE namespace=$1 AND name=$2 AND EXISTS(SELECT 1 FROM raddle_cluster_members applicant WHERE applicant.namespace=a.namespace AND applicant.instance=$3 AND applicant.expires_at>clock_timestamp()) AND (owner IS NULL OR lease_until<=clock_timestamp() OR NOT EXISTS(SELECT 1 FROM raddle_cluster_members m WHERE m.namespace=a.namespace AND m.instance=a.owner AND m.expires_at>clock_timestamp())) RETURNING epoch")
                .bind(&node.namespace).bind(agent).bind(node.instance).bind(milliseconds(self.options.lease_ttl, 7_200_000)?).fetch_optional(&mut *tx).await?;
            tx.commit().await?;
            Ok(epoch.map(|epoch| Lease { namespace: node.namespace.clone(), agent: agent.into(), owner: node.instance, epoch }))
        }).await
    }

    /// Acquires the reserved named leadership agent, if configured and available.
    pub async fn acquire_leadership(&self, node: &Node) -> Result<Option<Lease>, ClusterError> {
        self.acquire(node, LEADER_AGENT).await
    }

    /// Renews a live assignment only when owner, epoch, membership and unexpired
    /// lease all match. An expired owner cannot revive its lease even without takeover.
    pub async fn renew(&self, lease: &Lease) -> Result<(), ClusterError> {
        self.change_lease(lease, false).await
    }

    /// Releases a live matching assignment without deleting its epoch tombstone.
    pub async fn release(&self, lease: &Lease) -> Result<(), ClusterError> {
        self.change_lease(lease, true).await
    }

    async fn change_lease(&self, lease: &Lease, release: bool) -> Result<(), ClusterError> {
        self.lease_namespace(lease)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            self.live_member(&mut tx, lease.owner).await.map_err(|error| match error { ClusterError::DeadMember => ClusterError::StaleLease, other => other })?;
            let changed = sqlx::query("UPDATE raddle_cluster_agents a SET owner=CASE WHEN $5 THEN NULL ELSE owner END,lease_until=CASE WHEN $5 THEN NULL ELSE clock_timestamp()+$6::bigint*interval '1 millisecond' END WHERE namespace=$1 AND name=$2 AND owner=$3 AND epoch=$4 AND lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM raddle_cluster_members m WHERE m.namespace=a.namespace AND m.instance=$3 AND m.expires_at>clock_timestamp())")
                .bind(&lease.namespace).bind(&lease.agent).bind(lease.owner).bind(lease.epoch).bind(release).bind(milliseconds(self.options.lease_ttl, 7_200_000)?).execute(&mut *tx).await?;
            if changed.rows_affected()!=1 { return Err(ClusterError::StaleLease); }
            tx.commit().await?;
            Ok(())
        }).await
    }

    /// Gracefully releases this live node's agents and removes only its membership.
    /// No other node or task is stopped. All assignment epochs remain retained.
    pub async fn leave(&self, node: &Node) -> Result<(), ClusterError> {
        self.node_namespace(node)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            self.live_member(&mut tx, node.instance).await?;
            sqlx::query("UPDATE raddle_cluster_agents SET owner=NULL,lease_until=NULL WHERE namespace=$1 AND owner=$2")
                .bind(&node.namespace).bind(node.instance).execute(&mut *tx).await?;
            sqlx::query("DELETE FROM raddle_cluster_members WHERE namespace=$1 AND instance=$2")
                .bind(&node.namespace).bind(node.instance).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(())
        }).await
    }

    /// Removes at most 1024 expired members. Agents become eligible immediately
    /// when their member expires, even if this maintenance call is never made.
    pub async fn expire_dead(&self, limit: u32) -> Result<u64, ClusterError> {
        page(limit, 1024)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            self.lock(&mut tx, false).await?;
            let changed = sqlx::query("WITH expired AS (SELECT instance FROM raddle_cluster_members WHERE namespace=$1 AND expires_at<=clock_timestamp() ORDER BY instance LIMIT $2) DELETE FROM raddle_cluster_members m USING expired e WHERE m.namespace=$1 AND m.instance=e.instance")
                .bind(&self.options.namespace).bind(i64::from(limit)).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(changed.rows_affected())
        }).await
    }

    /// Reads a bounded live-membership snapshot, ordered by incarnation UUID.
    /// Diagnostic reads are not ownership checks or fencing capabilities.
    pub async fn members(&self, limit: u32) -> Result<Vec<Member>, ClusterError> {
        page(limit, 1024)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            let rows = sqlx::query("SELECT instance,label,roles,heartbeat_at,expires_at FROM raddle_cluster_members WHERE namespace=$1 AND expires_at>clock_timestamp() ORDER BY instance LIMIT $2")
                .bind(&self.options.namespace).bind(i64::from(limit)).fetch_all(&mut *tx).await?;
            let members = rows.into_iter().map(|row| Ok(Member { instance:row.try_get("instance")?, label:row.try_get("label")?, roles:row.try_get("roles")?, heartbeat_at:row.try_get("heartbeat_at")?, expires_at:row.try_get("expires_at")? })).collect::<Result<Vec<_>, sqlx::Error>>()?;
            tx.commit().await?;
            Ok(members)
        }).await
    }

    /// Reads at most 256 assignments, including persistent released tombstones.
    pub async fn assignments(&self, limit: u32) -> Result<Vec<Assignment>, ClusterError> {
        page(limit, 256)?;
        self.bounded(async {
            let mut tx = self.pool.begin().await?;
            self.deadlines(&mut tx).await?;
            let rows = sqlx::query("SELECT a.name,a.required_role,a.owner,a.epoch,a.lease_until,(a.lease_until>clock_timestamp() AND m.expires_at>clock_timestamp()) IS TRUE AS live FROM raddle_cluster_agents a LEFT JOIN raddle_cluster_members m ON m.namespace=a.namespace AND m.instance=a.owner WHERE a.namespace=$1 ORDER BY a.name LIMIT $2")
                .bind(&self.options.namespace).bind(i64::from(limit)).fetch_all(&mut *tx).await?;
            let assignments = rows.into_iter().map(|row| Ok(Assignment { agent:row.try_get("name")?, required_role:row.try_get("required_role")?, owner:row.try_get("owner")?, epoch:row.try_get("epoch")?, lease_until:row.try_get("lease_until")?, live:row.try_get("live")? })).collect::<Result<Vec<_>, sqlx::Error>>()?;
            tx.commit().await?;
            Ok(assignments)
        }).await
    }

    async fn bounded<T>(
        &self,
        future: impl Future<Output = Result<T, ClusterError>>,
    ) -> Result<T, ClusterError> {
        tokio::time::timeout(self.options.operation_timeout, future)
            .await
            .map_err(|_| ClusterError::Timeout)?
    }

    async fn deadlines(&self, connection: &mut PgConnection) -> Result<(), ClusterError> {
        // Server deadlines outlive cancellation of the Rust future. LOCAL avoids
        // contaminating application pool connections after commit or rollback.
        let timeout = format!("{}ms", self.options.operation_timeout.as_millis());
        sqlx::query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true),set_config('idle_in_transaction_session_timeout',$1,true)")
            .bind(timeout).execute(connection).await?;
        Ok(())
    }

    async fn lock(
        &self,
        connection: &mut PgConnection,
        fail_fast: bool,
    ) -> Result<(), ClusterError> {
        let query = if fail_fast {
            "SELECT configuration FROM raddle_cluster_namespaces WHERE namespace=$1 FOR UPDATE NOWAIT"
        } else {
            "SELECT configuration FROM raddle_cluster_namespaces WHERE namespace=$1 FOR UPDATE"
        };
        let config: Option<Value> = sqlx::query_scalar(query)
            .bind(&self.options.namespace)
            .fetch_optional(&mut *connection)
            .await?;
        if config.as_ref() != Some(&self.options.configuration()) {
            return Err(ClusterError::ConfigurationConflict);
        }
        // A tuple-version write also fences Repeatable Read snapshots. Without
        // it a stale scope could lock the root yet read pre-takeover agent data.
        sqlx::query(
            "UPDATE raddle_cluster_namespaces SET configuration=configuration WHERE namespace=$1",
        )
        .bind(&self.options.namespace)
        .execute(connection)
        .await?;
        Ok(())
    }

    async fn live_member(
        &self,
        connection: &mut PgConnection,
        instance: Uuid,
    ) -> Result<Vec<String>, ClusterError> {
        sqlx::query_scalar("SELECT roles FROM raddle_cluster_members WHERE namespace=$1 AND instance=$2 AND expires_at>clock_timestamp() FOR UPDATE")
            .bind(&self.options.namespace).bind(instance).fetch_optional(connection).await?.ok_or(ClusterError::DeadMember)
    }

    fn node_namespace(&self, node: &Node) -> Result<(), ClusterError> {
        if node.namespace != self.options.namespace {
            return Err(ClusterError::DeadMember);
        }
        Ok(())
    }

    fn lease_namespace(&self, lease: &Lease) -> Result<(), ClusterError> {
        if lease.namespace != self.options.namespace {
            return Err(ClusterError::StaleLease);
        }
        Ok(())
    }
}

impl Scope<'_> {
    /// Validates a private lease capability and holds its database fence until
    /// this SAME scope commits or rolls back. Call before touching business rows.
    /// A failed validation poisons the scope, including caller-side timeouts.
    /// Namespace contention fails fast (`55P03`); retry the entire transaction.
    ///
    /// The namespace lock serializes all its agents and membership mutations.
    /// Keep work short: LOCAL statement, lock and idle-transaction deadlines are
    /// set to the coordinator's operation budget for the rest of this scope.
    /// Takeover may occur after the lease deadline but never before this fence's
    /// transaction ends. External actions are not protected by this database lock.
    pub async fn enforce_agent(
        &mut self,
        cluster: &Cluster,
        lease: &Lease,
    ) -> Result<(), ClusterError> {
        // Drop also poisons the scope when the caller cancels this future while
        // PostgreSQL is processing a statement. Partial validation is never success.
        let mut validation = Validation {
            scope: self,
            accepted: false,
        };
        let result = cluster.bounded(async {
            cluster.lease_namespace(lease)?;
            cluster.deadlines(validation.scope.connection()).await?;
            cluster.lock(validation.scope.connection(), true).await?;
            cluster.live_member(validation.scope.connection(), lease.owner).await.map_err(|error| match error { ClusterError::DeadMember => ClusterError::StaleLease, other => other })?;
            let epoch: Option<i64> = sqlx::query_scalar("SELECT epoch FROM raddle_cluster_agents a WHERE namespace=$1 AND name=$2 AND owner=$3 AND epoch=$4 AND lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM raddle_cluster_members m WHERE m.namespace=a.namespace AND m.instance=$3 AND m.expires_at>clock_timestamp()) FOR UPDATE")
                .bind(&lease.namespace).bind(&lease.agent).bind(lease.owner).bind(lease.epoch).fetch_optional(validation.scope.connection()).await?;
            if epoch.is_none() { return Err(ClusterError::StaleLease); }
            Ok(())
        }).await;
        validation.accepted = result.is_ok();
        result
    }

    /// Alias for [`Scope::enforce_agent`]; validation holds the same transaction fence.
    pub async fn validate_lease(
        &mut self,
        cluster: &Cluster,
        lease: &Lease,
    ) -> Result<(), ClusterError> {
        self.enforce_agent(cluster, lease).await
    }
}

struct Validation<'scope, 'connection> {
    scope: &'scope mut Scope<'connection>,
    accepted: bool,
}

impl Drop for Validation<'_, '_> {
    fn drop(&mut self) {
        if !self.accepted {
            self.scope.mark_rollback_only();
        }
    }
}

fn identifier(value: &str) -> Result<(), ClusterError> {
    if value.is_empty()
        || value.len() > 64
        || !value.as_bytes()[0].is_ascii_lowercase()
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
    {
        return Err(ClusterError::Invalid("identifier"));
    }
    Ok(())
}

fn milliseconds(duration: Duration, maximum: u64) -> Result<i64, ClusterError> {
    if !duration.subsec_nanos().is_multiple_of(1_000_000)
        || duration < Duration::from_millis(1)
        || duration > Duration::from_millis(maximum)
    {
        return Err(ClusterError::Invalid("duration"));
    }
    Ok(duration.as_millis() as i64)
}

fn page(limit: u32, maximum: u32) -> Result<(), ClusterError> {
    if !(1..=maximum).contains(&limit) {
        return Err(ClusterError::Invalid("page size"));
    }
    Ok(())
}
