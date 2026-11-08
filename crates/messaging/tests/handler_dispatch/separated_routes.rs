//! Preconfigured routes must not create implicit combined separated executions.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raddle_host::Host;
use raddle_messaging::composition::{Group, Then};
use raddle_messaging::handler_routing::{
    Custody, EndpointOptions, HandlerSet, MultipleHandlerBehavior,
};
use raddle_messaging::scoped::LocalOnlyFlush;
use raddle_messaging::{DurableEndpoint, LocalQueue};
use raddle_postgres::{RetryPolicy, Store};
use sqlx::PgPool;

use super::{
    Member, ProducerOnly, Work, attach, builder, context, origin, rows, schema, settings, wait_rows,
};

#[tokio::test]
async fn separated_rejects_ambiguous_preconfigured_route_at_startup() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused/unused")
        .unwrap();
    let store = Store::new(pool);
    let mut builder = builder();
    builder.durable("orders", store.clone()).unwrap();
    builder.route::<Work>("orders").unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let chain = Then::new(
        Group::new("a", Arc::new(Member::<1>::new(&log))),
        Group::new("b", Arc::new(Member::<2>::new(&log))),
    );
    assert!(
        HandlerSet::<Work, _>::new(chain)
            .behavior(MultipleHandlerBehavior::Separated)
            .custody(Custody::Durable)
            .register(&mut builder, "orders", store, Arc::new(LocalOnlyFlush))
            .is_err(),
        "Separated must reject the extra orders route, not install a duplicate combined [a,b] chain"
    );
}

#[tokio::test]
async fn separated_preserves_outgoing_only_routes_without_installing_a_global_chain() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused/unused")
        .unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut builder = builder();
    builder.remote("upstream", ProducerOnly).unwrap();
    builder.route::<Work>("upstream").unwrap();
    let chain = Then::new(
        Group::new("a", Arc::new(Member::<1>::new(&log))),
        Group::new("b", Arc::new(Member::<2>::new(&log))),
    );
    let registration = HandlerSet::<Work, _>::new(chain)
        .behavior(MultipleHandlerBehavior::Separated)
        .register(
            &mut builder,
            "unused",
            Store::new(pool),
            Arc::new(LocalOnlyFlush),
        )
        .unwrap();
    builder.assert_no_combined_handlers().unwrap();
    assert!(registration.global.is_none());
    assert_eq!(registration.owners.local.len(), 2);
    builder.build().send(&Work(1), origin()).await.unwrap();
    assert!(
        registration
            .owners
            .local
            .iter()
            .all(|(_, queue, _)| queue.snapshot().buffered == 1)
    );
    assert!(log.lock().unwrap().is_empty());
}

#[derive(Clone, Copy, Debug)]
enum Assignment {
    Unassigned,
    OneElsewhere,
    AllElsewhere,
    SharedOrders,
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn rejected_existing_routes_leave_original_local_and_durable_bus_behavior_untouched(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut id = 0;
    for durable in [true, false] {
        for assignment in [
            Assignment::Unassigned,
            Assignment::OneElsewhere,
            Assignment::AllElsewhere,
            Assignment::SharedOrders,
        ] {
            id += 1;
            let log = Arc::new(Mutex::new(Vec::new()));
            let a = Arc::new(Member::<1>::new(&log));
            let b = Arc::new(Member::<2>::new(&log));
            let original = Arc::new(Member::<4>::new(&log));
            let mut builder = builder();
            let mut host = Host::new(Duration::from_secs(5)).unwrap();
            let mut existing = None;
            let mut local = None;
            if durable {
                builder.durable("orders", store.clone()).unwrap();
                let mut endpoint =
                    DurableEndpoint::new(store.clone(), "orders", 1, RetryPolicy::default())
                        .unwrap();
                endpoint
                    .handle::<Work, _>(Arc::clone(&original), Arc::new(LocalOnlyFlush))
                    .unwrap();
                existing = Some(endpoint);
            } else {
                let (queue, runner) = LocalQueue::new(settings()).unwrap();
                builder.local("orders", queue.clone()).unwrap();
                runner.attach(&mut host, "orders").unwrap();
                local = Some(queue);
            }
            builder.route::<Work>("orders").unwrap();
            let chain = Then::new(
                Group::new("a", Arc::clone(&a)),
                Group::new("b", Arc::clone(&b)),
            );
            let mut set = HandlerSet::<Work, _>::new(chain)
                .behavior(MultipleHandlerBehavior::Separated)
                .custody(Custody::Durable);
            match assignment {
                Assignment::Unassigned => {}
                Assignment::OneElsewhere => set = set.sticky("a", "sticky-a").unwrap(),
                Assignment::AllElsewhere => {
                    set = set
                        .sticky("a", "sticky-a")
                        .unwrap()
                        .sticky("b", "sticky-b")
                        .unwrap()
                }
                Assignment::SharedOrders => {
                    set = set
                        .sticky("a", "orders")
                        .unwrap()
                        .sticky("b", "orders")
                        .unwrap()
                }
            }
            assert!(
                set.register(
                    &mut builder,
                    "orders",
                    store.clone(),
                    Arc::new(LocalOnlyFlush)
                )
                .is_err(),
                "{durable:?} {assignment:?}"
            );
            assert!(builder.combined_handlers().is_empty());
            builder.assert_no_combined_handlers().unwrap();
            for absent in ["a", "b", "sticky-a", "sticky-b"] {
                assert!(
                    builder.route::<Work>(absent).is_err(),
                    "partial endpoint leaked: {absent}"
                );
            }
            if !durable {
                builder
                    .scoped::<Work, _>(
                        "orders",
                        Arc::clone(&original),
                        store.clone(),
                        Arc::new(LocalOnlyFlush),
                    )
                    .unwrap();
            }
            let bus = builder.build();
            if let Some(mut endpoint) = existing {
                let intent = bus.intent(&Work(id), &context(id), true).unwrap();
                assert_eq!(
                    intent
                        .deliveries
                        .iter()
                        .map(|delivery| delivery.destination.as_str())
                        .collect::<Vec<_>>(),
                    ["orders"]
                );
                endpoint.message_bus(bus.clone());
                endpoint
                    .attach(&mut host, Duration::from_millis(10))
                    .unwrap();
            }
            let stop = host.handle();
            let task = tokio::spawn(host.run());
            bus.send(&Work(id), origin()).await.unwrap();
            wait_rows(&pool, id as usize).await;
            assert!(stop.snapshot().running);
            assert!(
                stop.snapshot()
                    .services
                    .iter()
                    .all(|status| status.attempt == 1)
            );
            stop.shutdown();
            task.await.unwrap().unwrap();
            assert_eq!(original.created.load(Ordering::SeqCst), 1);
            assert_eq!(a.created.load(Ordering::SeqCst), 0);
            assert_eq!(b.created.load(Ordering::SeqCst), 0);
            if let Some(queue) = local {
                assert_eq!(queue.snapshot().completed, 1);
            }
        }
    }
    assert_eq!(
        rows(&pool).await,
        (1..=8).map(|id| (id, 4)).collect::<Vec<_>>()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE destination IN ('a','b','sticky-a','sticky-b')"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[derive(Clone, Copy, Debug)]
enum Binding {
    GroupName,
    StickyOrders,
    PrebuiltOrders,
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn explicit_single_group_routes_and_prebuilt_overrides_keep_failed_only_retries_live(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    for (position, binding) in [
        Binding::GroupName,
        Binding::StickyOrders,
        Binding::PrebuiltOrders,
    ]
    .into_iter()
    .enumerate()
    {
        let first = position as i32 * 3 + 1;
        let a_target = if matches!(binding, Binding::StickyOrders) {
            "orders"
        } else {
            "a"
        };
        let existing_name = if matches!(binding, Binding::GroupName) {
            "a"
        } else {
            "orders"
        };
        let log = Arc::new(Mutex::new(Vec::new()));
        let a = Arc::new(Member::<1>::new(&log));
        let b = Arc::new(Member::<2>::new(&log));
        let saga = Arc::new(Member::<3>::new(&log));
        let prebuilt = Arc::new(Member::<4>::new(&log));
        b.fail.store(first, Ordering::SeqCst);
        let mut builder = builder();
        builder.durable(existing_name, store.clone()).unwrap();
        builder.route::<Work>(existing_name).unwrap();
        let prebuilt_owners = if matches!(binding, Binding::PrebuiltOrders) {
            Some(
                builder
                    .sticky_handler::<Work, _>(
                        "orders",
                        Arc::clone(&prebuilt),
                        store.clone(),
                        Arc::new(LocalOnlyFlush),
                        EndpointOptions::default(),
                    )
                    .unwrap(),
            )
        } else {
            None
        };
        let chain = Then::new(
            Then::new(
                Group::new("a", Arc::clone(&a)),
                Group::new("b", Arc::clone(&b)),
            ),
            Group::new("saga", Arc::clone(&saga)),
        );
        let mut set = HandlerSet::<Work, _>::new(chain)
            .behavior(MultipleHandlerBehavior::Separated)
            .custody(Custody::Durable);
        if matches!(binding, Binding::StickyOrders) {
            set = set.sticky("a", "orders").unwrap();
        }
        let mut b_options = EndpointOptions::default();
        b_options.parallelism = 3;
        b_options.retry = RetryPolicy::try_new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap();
        set = set.endpoint_options("b", b_options).unwrap();
        let registration = set
            .register(
                &mut builder,
                "orders",
                store.clone(),
                Arc::new(LocalOnlyFlush),
            )
            .unwrap();
        assert!(registration.global.is_none());
        builder.assert_no_combined_handlers().unwrap();
        let bus = builder.build();
        let intent = bus.intent(&Work(first), &context(first), true).unwrap();
        let actual = intent
            .deliveries
            .iter()
            .map(|delivery| delivery.destination.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut expected = std::collections::BTreeSet::from([a_target, "b", "saga"]);
        if matches!(binding, Binding::PrebuiltOrders) {
            expected.insert("orders");
        }
        assert_eq!(actual, expected, "{binding:?}");
        assert_eq!(intent.deliveries.len(), expected.len());
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        attach(registration.owners, &bus, &mut host);
        if let Some(owners) = prebuilt_owners {
            attach(owners, &bus, &mut host);
        }
        let stop = host.handle();
        let task = tokio::spawn(host.run());
        let earlier = rows(&pool).await.len();
        let per_message = expected.len();
        bus.send(&Work(first), origin()).await.unwrap();
        bus.send(&Work(first + 1), origin()).await.unwrap();
        wait_rows(&pool, earlier + per_message * 2 - 1).await;
        bus.send(&Work(first + 2), origin()).await.unwrap();
        wait_rows(&pool, earlier + per_message * 3 - 1).await;
        assert!(stop.snapshot().running);
        assert!(
            stop.snapshot()
                .services
                .iter()
                .all(|status| status.attempt == 1)
        );
        b.fail.store(-1, Ordering::SeqCst);
        wait_rows(&pool, earlier + per_message * 3).await;
        stop.shutdown();
        task.await.unwrap().unwrap();
        assert_eq!(a.created.load(Ordering::SeqCst), 3);
        assert_eq!(saga.created.load(Ordering::SeqCst), 3);
        assert!(b.created.load(Ordering::SeqCst) >= 4);
        assert_eq!(
            prebuilt.created.load(Ordering::SeqCst),
            if matches!(binding, Binding::PrebuiltOrders) {
                3
            } else {
                0
            }
        );
        let facts: Vec<(i32,i32,i64)> = sqlx::query_as("SELECT id,member,count(*) FROM facts WHERE phase='handle' AND id BETWEEN $1 AND $2 GROUP BY id,member ORDER BY id,member").bind(first).bind(first+2).fetch_all(&pool).await.unwrap();
        assert_eq!(facts.len(), per_message * 3);
        assert!(facts.iter().all(|(_, _, count)| *count == 1));
        let states: Vec<serde_json::Value> =
            sqlx::query_scalar("SELECT state FROM raddle_sagas WHERE correlation=ANY($1)")
                .bind(
                    (first..first + 3)
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>(),
                )
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(states, vec![serde_json::json!(1); 3]);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination IN ('a','orders','saga') AND (attempts<>1 OR status<>'completed')").fetch_one(&pool).await.unwrap(),0);
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn classic_adopts_existing_orders_route_as_one_transaction_without_extra_subscriptions(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = Arc::new(Member::<1>::new(&log));
    let b = Arc::new(Member::<2>::new(&log));
    b.requires_first.store(true, Ordering::SeqCst);
    let mut builder = builder();
    builder.durable("orders", store.clone()).unwrap();
    builder.route::<Work>("orders").unwrap();
    let registration = HandlerSet::<Work, _>::new(Then::new(
        Group::new("a", Arc::clone(&a)),
        Group::new("b", Arc::clone(&b)),
    ))
    .register(&mut builder, "orders", store, Arc::new(LocalOnlyFlush))
    .unwrap();
    assert_eq!(registration.owners.durable.len(), 1);
    assert_eq!(builder.combined_handlers()[0].groups, ["a", "b"]);
    let bus = builder.build();
    assert_eq!(
        bus.intent(&Work(1), &context(1), true)
            .unwrap()
            .deliveries
            .len(),
        1
    );
    bus.send(&Work(1), origin()).await.unwrap();
    assert_eq!(
        registration.owners.durable[0].1.run_once().await.unwrap(),
        1
    );
    assert_eq!(rows(&pool).await, [(1, 1), (1, 2)]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT tx) FROM facts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM raddle_jobs WHERE destination='orders' AND status='completed' AND attempts=1").fetch_one(&pool).await.unwrap(),1);
}
