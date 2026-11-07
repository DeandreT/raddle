//! Opt-in multi-node tests against isolated databases on disposable PostgreSQL.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{self, Command};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle_postgres::cluster::{AgentSpec, Cluster, ClusterError, ClusterOptions, LEADER_AGENT};
use raddle_postgres::{Error, Scope};
use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::sync::Barrier;

fn options(namespace: &str) -> ClusterOptions {
    ClusterOptions::new(
        namespace,
        &["worker", "observer"],
        vec![
            AgentSpec::new("cleanup", "worker").unwrap(),
            AgentSpec::new("scheduler", "worker").unwrap(),
            AgentSpec::leader("worker").unwrap(),
        ],
    )
    .unwrap()
    .with_timing(
        Duration::from_secs(600),
        Duration::from_secs(300),
        Duration::from_secs(10),
    )
    .unwrap()
}

async fn peer_pool(pool: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap()
}

async fn expire_member(pool: &PgPool, instance: uuid::Uuid) {
    sqlx::query("UPDATE raddle_cluster_members SET expires_at=clock_timestamp()-interval '1 second' WHERE instance=$1")
        .bind(instance).execute(pool).await.unwrap();
}

async fn expire_agent(pool: &PgPool, agent: &str) {
    sqlx::query("UPDATE raddle_cluster_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE name=$1")
        .bind(agent).execute(pool).await.unwrap();
}

#[test]
fn names_roles_and_budgets_are_validated_before_database_work() {
    for name in [
        "",
        "Tenant",
        "bad/name",
        "a\n",
        "_leader",
        "x'; DROP TABLE users; --",
    ] {
        assert!(AgentSpec::new(name, "worker").is_err());
        assert!(
            ClusterOptions::new(
                name,
                &["worker"],
                vec![AgentSpec::leader("worker").unwrap()]
            )
            .is_err()
        );
    }
    assert!(AgentSpec::new(&"a".repeat(65), "worker").is_err());
    assert!(AgentSpec::new("agent", "Admin").is_err());
    assert!(
        ClusterOptions::new(
            "test",
            &["worker"],
            vec![AgentSpec::leader("unknown").unwrap()]
        )
        .is_err()
    );
    assert!(
        ClusterOptions::new(
            "test",
            &["worker", "worker"],
            vec![AgentSpec::leader("worker").unwrap()]
        )
        .is_err()
    );
    assert!(ClusterOptions::new("test", &["worker"], vec![]).is_err());
    assert!(
        ClusterOptions::new(
            "test",
            &["worker"],
            vec![AgentSpec::leader("worker").unwrap(); 257]
        )
        .is_err()
    );
    assert!(options("test").with_max_members(0).is_err());
    assert!(options("test").with_max_members(1025).is_err());
    for invalid in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(7201),
    ] {
        assert!(
            options("test")
                .with_timing(invalid, Duration::from_secs(1), Duration::from_secs(1))
                .is_err()
        );
    }
    assert!(
        options("test")
            .with_timing(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(31)
            )
            .is_err()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_nodes_have_one_agent_and_one_leader_winner(pool: PgPool) {
    let peer = peer_pool(&pool).await;
    let first = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let second = Cluster::connect(peer.clone(), options("test"))
        .await
        .unwrap();
    let a = first.register("node-a", &["worker"]).await.unwrap();
    let b = second.register("node-b", &["worker"]).await.unwrap();
    assert_ne!(a.instance(), b.instance());
    let gate = Arc::new(Barrier::new(2));
    let (left, right) = tokio::join!(
        async {
            gate.wait().await;
            first.acquire(&a, "scheduler").await.unwrap()
        },
        async {
            gate.wait().await;
            second.acquire(&b, "scheduler").await.unwrap()
        },
    );
    assert_eq!(
        usize::from(left.is_some()) + usize::from(right.is_some()),
        1
    );
    assert_eq!(left.or(right).unwrap().epoch(), 1);
    let (left, right) = tokio::join!(first.acquire_leadership(&a), second.acquire_leadership(&b));
    let (left, right) = (left.unwrap(), right.unwrap());
    assert_eq!(
        usize::from(left.is_some()) + usize::from(right.is_some()),
        1
    );
    assert_eq!(left.or(right).unwrap().agent(), LEADER_AGENT);
    assert_eq!(first.members(1).await.unwrap().len(), 1);
    assert_eq!(first.assignments(2).await.unwrap().len(), 2);
    assert_eq!(
        first
            .assignments(256)
            .await
            .unwrap()
            .iter()
            .filter(|a| a.live)
            .count(),
        2
    );
    peer.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn heartbeat_uses_database_clock_and_dead_members_cannot_revive(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("same-machine", &["worker"]).await.unwrap();
    // A past server value makes the heartbeat change observable without sleeping.
    sqlx::query("UPDATE raddle_cluster_members SET heartbeat_at=clock_timestamp()-interval '1 hour' WHERE instance=$1")
        .bind(node.instance()).execute(&pool).await.unwrap();
    let before: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    cluster.heartbeat(&node).await.unwrap();
    let after: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let member = cluster.members(10).await.unwrap().remove(0);
    assert!(member.heartbeat_at >= before && member.heartbeat_at <= after);
    assert_eq!((member.expires_at - member.heartbeat_at).num_seconds(), 600);
    expire_member(&pool, node.instance()).await;
    assert!(matches!(
        cluster.heartbeat(&node).await,
        Err(ClusterError::DeadMember)
    ));
    assert!(matches!(
        cluster.acquire(&node, "cleanup").await,
        Err(ClusterError::DeadMember)
    ));
    assert!(cluster.members(10).await.unwrap().is_empty());
    assert_eq!(cluster.expire_dead(1).await.unwrap(), 1);
    assert_eq!(cluster.expire_dead(1).await.unwrap(), 0);
    let restarted = cluster.register("same-machine", &["worker"]).await.unwrap();
    assert_ne!(node.instance(), restarted.instance());
    assert!(
        cluster
            .acquire(&restarted, "cleanup")
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        cluster.heartbeat(&node).await,
        Err(ClusterError::DeadMember)
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn dead_members_allow_takeover_before_agent_deadline_and_reject_old_fence(pool: PgPool) {
    let peer = peer_pool(&pool).await;
    let first = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let second = Cluster::connect(peer.clone(), options("test"))
        .await
        .unwrap();
    let a = first.register("node-a", &["worker"]).await.unwrap();
    let b = second.register("node-b", &["worker"]).await.unwrap();
    let old = first.acquire(&a, "cleanup").await.unwrap().unwrap();
    expire_member(&pool, a.instance()).await;
    let assignment = first
        .assignments(256)
        .await
        .unwrap()
        .into_iter()
        .find(|a| a.agent == "cleanup")
        .unwrap();
    assert!(!assignment.live);
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(assignment.lease_until.unwrap() > database_now);
    let replacement = second.acquire(&b, "cleanup").await.unwrap().unwrap();
    assert_eq!(replacement.epoch(), old.epoch() + 1);
    assert!(matches!(
        first.renew(&old).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(matches!(
        first.release(&old).await,
        Err(ClusterError::StaleLease)
    ));
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    assert!(matches!(
        scope.enforce_agent(&first, &old).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    second.renew(&replacement).await.unwrap();
    peer.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expired_lease_cannot_renew_or_release_even_without_takeover(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let a = cluster.register("a", &["worker"]).await.unwrap();
    let b = cluster.register("b", &["worker"]).await.unwrap();
    let old = cluster.acquire(&a, "cleanup").await.unwrap().unwrap();
    expire_agent(&pool, "cleanup").await;
    assert!(matches!(
        cluster.renew(&old).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(matches!(
        cluster.release(&old).await,
        Err(ClusterError::StaleLease)
    ));
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    assert!(matches!(
        scope.validate_lease(&cluster, &old).await,
        Err(ClusterError::StaleLease)
    ));
    scope.rollback().await.unwrap();
    let fresh = cluster.acquire(&b, "cleanup").await.unwrap().unwrap();
    assert_eq!(fresh.epoch(), old.epoch() + 1);
    assert!(matches!(
        cluster.release(&old).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(cluster.acquire(&a, "cleanup").await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn released_and_graceful_leave_tombstones_never_reset_epoch(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let first = cluster.register("same-label", &["worker"]).await.unwrap();
    let old = cluster.acquire(&first, "cleanup").await.unwrap().unwrap();
    cluster.release(&old).await.unwrap();
    let tombstone = cluster
        .assignments(256)
        .await
        .unwrap()
        .into_iter()
        .find(|a| a.agent == "cleanup")
        .unwrap();
    assert_eq!(tombstone.epoch, old.epoch());
    assert!(tombstone.owner.is_none() && tombstone.lease_until.is_none() && !tombstone.live);
    let next = cluster.acquire(&first, "cleanup").await.unwrap().unwrap();
    assert_eq!(next.epoch(), old.epoch() + 1);
    assert!(matches!(
        cluster.renew(&old).await,
        Err(ClusterError::StaleLease)
    ));
    let leader = cluster.acquire_leadership(&first).await.unwrap().unwrap();
    cluster.leave(&first).await.unwrap();
    assert!(matches!(
        cluster.heartbeat(&first).await,
        Err(ClusterError::DeadMember)
    ));
    let peer = peer_pool(&pool).await;
    let restarted = Cluster::connect(peer.clone(), options("test"))
        .await
        .unwrap();
    let node = restarted.register("same-label", &["worker"]).await.unwrap();
    assert_ne!(node.instance(), first.instance());
    assert_eq!(
        restarted
            .acquire(&node, "cleanup")
            .await
            .unwrap()
            .unwrap()
            .epoch(),
        next.epoch() + 1
    );
    assert_eq!(
        restarted
            .acquire_leadership(&node)
            .await
            .unwrap()
            .unwrap()
            .epoch(),
        leader.epoch() + 1
    );
    assert!(matches!(
        restarted.release(&next).await,
        Err(ClusterError::StaleLease)
    ));
    peer.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn business_commit_rollback_and_stale_validation_use_the_same_scope(pool: PgPool) {
    sqlx::query("CREATE TABLE business(value integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    let lease = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope.enforce_agent(&cluster, &lease).await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope.validate_lease(&cluster, &lease).await.unwrap();
    sqlx::query("INSERT INTO business VALUES(2)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.rollback().await.unwrap();
    cluster.release(&lease).await.unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    sqlx::query("INSERT INTO business VALUES(3)")
        .execute(scope.connection())
        .await
        .unwrap();
    assert!(matches!(
        scope.enforce_agent(&cluster, &lease).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT value FROM business")
            .fetch_all(&pool)
            .await
            .unwrap(),
        vec![1]
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expired_assignment_cannot_take_over_until_fenced_business_commits(pool: PgPool) {
    sqlx::query("CREATE TABLE business(value integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let peer = peer_pool(&pool).await;
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let challenger = Cluster::connect(peer.clone(), options("test"))
        .await
        .unwrap();
    let a = cluster.register("a", &["worker"]).await.unwrap();
    let b = challenger.register("b", &["worker"]).await.unwrap();
    let lease = cluster.acquire(&a, "cleanup").await.unwrap().unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope.enforce_agent(&cluster, &lease).await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    // Controlled expiry in this very transaction, without waiting for a long TTL.
    sqlx::query("UPDATE raddle_cluster_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE name='cleanup'")
        .execute(scope.connection()).await.unwrap();
    let mut takeover = Box::pin(challenger.acquire(&b, "cleanup"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut takeover)
            .await
            .is_err()
    );
    scope.commit(Utc::now()).await.unwrap();
    let replacement = takeover.await.unwrap().unwrap();
    assert_eq!(replacement.epoch(), lease.epoch() + 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
            .fetch_one(&peer)
            .await
            .unwrap(),
        1
    );
    let mut stale = Scope::enroll(pool.begin().await.unwrap());
    assert!(matches!(
        stale.enforce_agent(&cluster, &lease).await,
        Err(ClusterError::StaleLease)
    ));
    stale.rollback().await.unwrap();
    peer.close().await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn all_agents_use_one_lock_order_and_contending_scopes_fail_fast(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    let cleanup = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    let scheduler = cluster.acquire(&node, "scheduler").await.unwrap().unwrap();
    let mut first = Scope::enroll(pool.begin().await.unwrap());
    first.enforce_agent(&cluster, &cleanup).await.unwrap();
    first.enforce_agent(&cluster, &scheduler).await.unwrap();
    let mut second = Scope::enroll(pool.begin().await.unwrap());
    let error = second
        .enforce_agent(&cluster, &scheduler)
        .await
        .unwrap_err();
    match error {
        ClusterError::Database(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("55P03"))
        }
        other => panic!("expected fail-fast namespace contention, got {other}"),
    }
    second.rollback().await.unwrap();
    first.rollback().await.unwrap();
    cluster.renew(&cleanup).await.unwrap();
    cluster.renew(&scheduler).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn stale_repeatable_read_snapshot_cannot_validate_pre_takeover_owner(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    let old = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT count(*) FROM raddle_cluster_agents")
        .execute(&mut *tx)
        .await
        .unwrap();
    cluster.release(&old).await.unwrap();
    let next = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    assert!(next.epoch() > old.epoch());
    let mut scope = Scope::enroll(tx);
    match scope.enforce_agent(&cluster, &old).await.unwrap_err() {
        ClusterError::Database(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("40001"))
        }
        other => panic!("expected serialization failure, got {other}"),
    }
    scope.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn membership_capacity_roles_namespace_and_configuration_are_enforced(pool: PgPool) {
    let policy = options("test").with_max_members(1).unwrap();
    let cluster = Cluster::connect(pool.clone(), policy.clone())
        .await
        .unwrap();
    assert!(matches!(
        cluster.register("a", &["admin"]).await,
        Err(ClusterError::Invalid(_))
    ));
    assert!(matches!(
        cluster.register("\n", &["worker"]).await,
        Err(ClusterError::Invalid(_))
    ));
    assert!(matches!(
        cluster.register(&"a".repeat(129), &["worker"]).await,
        Err(ClusterError::Invalid(_))
    ));
    assert!(matches!(
        cluster.register("a", &[]).await,
        Err(ClusterError::Invalid(_))
    ));
    let observer = cluster.register("observer", &["observer"]).await.unwrap();
    assert!(matches!(
        cluster.acquire(&observer, "cleanup").await,
        Err(ClusterError::Invalid(_))
    ));
    assert!(matches!(
        cluster.register("b", &["worker"]).await,
        Err(ClusterError::Capacity)
    ));
    expire_member(&pool, observer.instance()).await;
    let worker = cluster.register("worker", &["worker"]).await.unwrap();
    assert_eq!(cluster.members(10).await.unwrap().len(), 1);
    assert!(matches!(
        cluster.acquire(&worker, "unconfigured").await,
        Err(ClusterError::Invalid(_))
    ));
    let lease = cluster.acquire(&worker, "cleanup").await.unwrap().unwrap();
    let other = Cluster::connect(pool.clone(), options("other"))
        .await
        .unwrap();
    assert!(matches!(
        other.heartbeat(&worker).await,
        Err(ClusterError::DeadMember)
    ));
    assert!(matches!(
        other.renew(&lease).await,
        Err(ClusterError::StaleLease)
    ));
    assert!(matches!(
        Cluster::connect(pool.clone(), options("test")).await,
        Err(ClusterError::ConfigurationConflict)
    ));
    assert!(cluster.members(0).await.is_err());
    assert!(cluster.members(1025).await.is_err());
    assert!(cluster.assignments(257).await.is_err());
    assert!(cluster.expire_dead(0).await.is_err());
    cluster.leave(&worker).await.unwrap();
    assert!(cluster.members(10).await.unwrap().is_empty());
    Cluster::connect(pool.clone(), policy).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn epoch_exhaustion_fails_closed_without_wrapping_or_erasing_tombstone(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    sqlx::query("UPDATE raddle_cluster_agents SET epoch=$1 WHERE name='cleanup'")
        .bind(i64::MAX)
        .execute(&pool)
        .await
        .unwrap();
    match cluster.acquire(&node, "cleanup").await.unwrap_err() {
        ClusterError::Database(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("22003"))
        }
        other => panic!("expected epoch overflow rejection, got {other}"),
    }
    let assignment = cluster
        .assignments(256)
        .await
        .unwrap()
        .into_iter()
        .find(|a| a.agent == "cleanup")
        .unwrap();
    assert_eq!(assignment.epoch, i64::MAX);
    assert!(assignment.owner.is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn operation_and_server_deadlines_bound_namespace_contention(pool: PgPool) {
    let policy = options("test");
    let cluster = Cluster::connect(pool.clone(), policy.clone())
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    let lease = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    let impatient = Cluster::connect(
        pool.clone(),
        policy
            .with_timing(
                Duration::from_secs(600),
                Duration::from_secs(300),
                Duration::from_millis(100),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope.enforce_agent(&cluster, &lease).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), impatient.heartbeat(&node))
        .await
        .unwrap()
        .unwrap_err();
    match error {
        ClusterError::Timeout => {}
        ClusterError::Database(sqlx::Error::Database(error)) => {
            assert!(matches!(error.code().as_deref(), Some("55P03" | "57014")))
        }
        other => panic!("expected bounded lock/query timeout, got {other}"),
    }
    scope.rollback().await.unwrap();
    cluster.heartbeat(&node).await.unwrap();
    assert!(cluster.acquire(&node, "cleanup").await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelling_validation_poison_scope_without_stopping_another_member(pool: PgPool) {
    let cluster = Cluster::connect(pool.clone(), options("test"))
        .await
        .unwrap();
    let node = cluster.register("a", &["worker"]).await.unwrap();
    let other = cluster.register("b", &["worker"]).await.unwrap();
    let lease = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    // Poll validation until its first network suspension, then cancel it. The
    // losing future is dropped while its transaction is still owned by scope.
    tokio::select! {
        biased;
        result = scope.enforce_agent(&cluster, &lease) => panic!("validation unexpectedly completed in one poll: {result:?}"),
        () = std::future::ready(()) => {},
    }
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    cluster.heartbeat(&other).await.unwrap();
    assert!(
        cluster
            .acquire(&other, "scheduler")
            .await
            .unwrap()
            .is_some()
    );
    cluster.renew(&lease).await.unwrap();
}

// The process helper is inert during ordinary tests. Its own deadline bounds
// startup and database work, and it exits without running Rust destructors.
#[tokio::test]
async fn subprocess_cluster_crash_probe() {
    let Ok(url) = std::env::var("RADDLE_CLUSTER_CRASH_URL") else {
        return;
    };
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let connect: PgConnectOptions = url.parse().unwrap();
        let pool = PgPool::connect_with(connect.disable_statement_logging())
            .await
            .unwrap();
        let cluster = Cluster::connect(pool.clone(), options("crash"))
            .await
            .unwrap();
        let node = cluster
            .register("restartable-node", &["worker"])
            .await
            .unwrap();
        let lease = cluster.acquire(&node, "cleanup").await.unwrap().unwrap();
        sqlx::query("INSERT INTO cluster_exit VALUES($1,$2)")
            .bind(node.instance())
            .bind(lease.epoch())
            .execute(&pool)
            .await
            .unwrap();
        let mut scope = Scope::enroll(pool.begin().await.unwrap());
        scope.enforce_agent(&cluster, &lease).await.unwrap();
        sqlx::query("INSERT INTO business VALUES(99)")
            .execute(scope.connection())
            .await
            .unwrap();
        process::exit(29);
    })
    .await;
    panic!("crash probe missed intended exit: {result:?}");
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn abrupt_process_exit_rolls_back_fenced_work_and_restart_gets_new_owner(pool: PgPool) {
    sqlx::query("CREATE TABLE cluster_exit(instance uuid,epoch bigint)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE business(value integer)")
        .execute(&pool)
        .await
        .unwrap();
    let url = pool.connect_options().to_url_lossy().to_string();
    let outcome = tokio::task::spawn_blocking(move || {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "subprocess_cluster_crash_probe", "--nocapture"])
            .env("RADDLE_CLUSTER_CRASH_URL", url)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(
        outcome.status.code(),
        Some(29),
        "subprocess failed to reach its intended crash point"
    );
    let (old_owner, old_epoch): (uuid::Uuid, i64) =
        sqlx::query_as("SELECT instance,epoch FROM cluster_exit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM business")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    expire_member(&pool, old_owner).await;
    let restarted = Cluster::connect(pool.clone(), options("crash"))
        .await
        .unwrap();
    let node = restarted
        .register("restartable-node", &["worker"])
        .await
        .unwrap();
    let lease = restarted.acquire(&node, "cleanup").await.unwrap().unwrap();
    assert_ne!(lease.owner(), old_owner);
    assert_eq!(lease.epoch(), old_epoch + 1);
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope.enforce_agent(&restarted, &lease).await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT value FROM business")
            .fetch_all(&pool)
            .await
            .unwrap(),
        vec![1]
    );
}
