//! Shared executable/test application. This is example code, not a framework API.

#[path = "cleanup.rs"]
mod cleanup;

use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use lapin::message::Delivery as BrokerDelivery;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
    ExchangeDeleteOptions, QueueDeleteOptions,
};
use lapin::{BasicProperties, Confirmation, Connection as AmqpConnection, ConnectionProperties};
use raddle::pipeline::Pipeline;
use raddle::wire::{Codecs, Envelope, WireError, WireMessage};
use raddle::{Request, RuntimeBuilder};
use raddle_host::{Host, HostHandle, PollSettings, RestartPolicy, ServiceContext, ServiceFailure};
use raddle_postgres::events::{EventError, EventStream, ExpectedRevision, ProjectionProgress};
use raddle_postgres::{
    CorrelationKey, Delivery, DeliveryKind, HandlerFailure, MissedTickPolicy, OutboxWorker,
    RecurringSchedule, RecurringScheduler, RetryPolicy, Saga, SagaInitialization, SagaState,
    ScheduleKey, Scope, Store, Worker,
};
use raddle_rabbitmq::{
    Binding, Connection, Exchange, ExchangeKind, Publisher, Queue, Route, Topology,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use cleanup::{Budget, Operation};

/// Errors are returned through the caller's joined cleanup boundary.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const SUBSCRIPTION: &str = "application.place-order";
const DESTINATION: &str = "application.accepted";
const SOURCE: &str = "application.example";
const PROJECTION: &str = "application.order-view";
const FORGED_TENANT: &str = "payload-is-not-a-tenant-principal";

/// Observable results, returned only after joining services and deleting broker entities.
pub struct Summary {
    /// Trusted configured namespace, unique per invocation.
    pub tenant: String,
    /// Valid inputs, before and after the rejected input.
    pub accepted: usize,
    /// Typed pipeline validation failures, never durably admitted.
    pub rejected: usize,
    /// Persisted recurring occurrences emitted by the hosted scheduler.
    pub scheduled: usize,
    /// Completed local and confirmed outbox jobs.
    pub completed_jobs: i64,
    /// Read-model rows with their transactional checkpoints.
    pub projected: i64,
    /// Successful explicit acknowledgement calls after durable receipt commits.
    pub settled: usize,
    /// Malformed fixture deliveries durably quarantined before acknowledgement.
    pub quarantined: usize,
    /// Identities shared by each event, its outbox job, and its broker delivery.
    pub event_ids: Vec<Uuid>,
    /// Rejected input identity, for independent absence checks.
    pub rejected_order: Uuid,
}

struct SubmitOrder {
    order_id: Uuid,
    units: i32,
}

#[derive(Debug, thiserror::Error)]
#[error("order units must be within 1..=1000 and its identity must be non-nil")]
struct InvalidOrder;

impl Request for SubmitOrder {
    type Reply = PlaceOrder;
    type Error = InvalidOrder;
}

#[derive(Clone, Serialize, Deserialize)]
struct PlaceOrder {
    order_id: Uuid,
    units: i32,
    event_id: Uuid,
    // Deliberately misleading input; only Scope::tenant supplies authority.
    tenant_hint: String,
}
impl WireMessage for PlaceOrder {
    const NAME: &'static str = "example.place-order";
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct OrderAccepted {
    tenant: String,
    order_id: Uuid,
    units: i32,
    command_id: Uuid,
}
impl WireMessage for OrderAccepted {
    const NAME: &'static str = "example.order-accepted";
    const VERSION: u32 = 1;
}

#[derive(Default, Serialize, Deserialize)]
struct OrderSaga {
    accepted_units: i32,
}
impl SagaState for OrderSaga {
    const NAME: &'static str = "example.order-saga";
    const VERSION: u32 = 1;
}

#[derive(Default)]
struct Metrics {
    scheduled: AtomicUsize,
    settled: AtomicUsize,
    schedule_failures: AtomicUsize,
    projection_failures: AtomicUsize,
    quarantined: AtomicUsize,
}

struct Plan {
    tenant: String,
    commands: Vec<PlaceOrder>,
    keys: Vec<ScheduleKey>,
    rejected_order: Uuid,
}

/// Requires numeric loopback RabbitMQ and local PostgreSQL (TCP or Unix socket).
pub fn validate_endpoints(database_url: &str, rabbit_uri: &str) -> Result<()> {
    let database = url::Url::parse(database_url).map_err(|_| "invalid disposable database URI")?;
    ensure(
        matches!(database.scheme(), "postgres" | "postgresql") && database.fragment().is_none(),
        "invalid disposable database URI",
    )?;
    let options: PgConnectOptions = database_url
        .parse()
        .map_err(|_| "invalid disposable database URI")?;
    validate_database(&options)?;
    validate_uri(rabbit_uri, &["amqp", "amqps"])
}

fn validate_database(options: &PgConnectOptions) -> Result<()> {
    let local = match options.get_socket() {
        Some(socket) => socket.is_absolute(),
        None => options
            .get_host()
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
    };
    ensure(
        local,
        "the application example requires local PostgreSQL via numeric loopback or an absolute Unix socket",
    )
}

fn validate_uri(uri: &str, schemes: &[&str]) -> Result<()> {
    let parsed = url::Url::parse(uri).map_err(|_| "invalid disposable service URI")?;
    let loopback = match parsed.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
        None => false,
    };
    ensure(
        loopback && schemes.contains(&parsed.scheme()) && parsed.fragment().is_none(),
        "the application example requires explicitly selected numeric loopback services",
    )
}

/// Executes the same real application used by the opt-in SQLx integration test.
/// The pool must target a disposable migrated local database; RabbitMQ must use
/// numeric loopback. Normal failures still join/close services,
/// cancel this run's schedules, and attempt deletion of both owned broker names.
pub async fn run(pool: &PgPool, rabbit_uri: &str) -> Result<Summary> {
    validate_uri(rabbit_uri, &["amqp", "amqps"])?;
    validate_database(&pool.connect_options())?;
    let plan = plan().await?;
    let broker = Broker::connect(rabbit_uri).await?;
    let store = Store::new(pool.clone());
    let mut publisher_cleanup = None;
    let result = async {
        broker.declare().await?;
        execute(pool, &store, &plan, &broker, &mut publisher_cleanup).await
    }
    .await;
    let cleaned = broker.close(&store, &plan.keys, publisher_cleanup).await;
    let summary = result?;
    cleaned?;
    Ok(summary)
}

async fn plan() -> Result<Plan> {
    let pipeline = Pipeline::new(|input: SubmitOrder, _| async move {
        Ok(PlaceOrder {
            order_id: input.order_id,
            units: input.units,
            event_id: Uuid::new_v4(),
            tenant_hint: FORGED_TENANT.into(),
        })
    })
    .validate(|input: SubmitOrder, _| async move {
        if input.order_id.is_nil() || !(1..=1000).contains(&input.units) {
            Err(InvalidOrder)
        } else {
            Ok(input)
        }
    });
    let mut builder = RuntimeBuilder::try_new(2)?;
    builder.handle_pipeline(pipeline)?;
    let runtime = builder.build();
    let rejected_order = Uuid::new_v4();
    let mut commands = Vec::new();
    let mut rejected = 0;
    for (order_id, units) in [
        (Uuid::new_v4(), 3),
        (rejected_order, 0),
        (Uuid::new_v4(), 7),
    ] {
        match runtime.invoke(SubmitOrder { order_id, units }).await {
            Ok(command) => commands.push(command),
            Err(raddle::ExecutionError::Handler(_)) => rejected += 1,
            Err(error) => return Err(error.into()),
        }
    }
    ensure(
        commands.len() == 2 && rejected == 1,
        "pipeline failed input isolation",
    )?;
    let tenant = format!("application-{}", Uuid::new_v4());
    let keys = commands
        .iter()
        .map(|command| {
            ScheduleKey::new(
                &tenant,
                SOURCE,
                &format!("order-{}", command.order_id.simple()),
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Plan {
        tenant,
        commands,
        keys,
        rejected_order,
    })
}

async fn execute(
    pool: &PgPool,
    store: &Store,
    plan: &Plan,
    broker: &Broker,
    publisher_cleanup: &mut Option<Publisher>,
) -> Result<Summary> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS raddle_example_orders (
        tenant text NOT NULL, order_id uuid NOT NULL, units integer NOT NULL,
        command_id uuid NOT NULL, event_id uuid NOT NULL, PRIMARY KEY(tenant,order_id));
        CREATE TABLE IF NOT EXISTS raddle_example_order_view (
        tenant text NOT NULL, order_id uuid NOT NULL, units integer NOT NULL,
        event_id uuid NOT NULL, apply_count integer NOT NULL, PRIMARY KEY(tenant,order_id));
        CREATE TABLE IF NOT EXISTS raddle_example_receipts (
        tenant text NOT NULL, event_id uuid NOT NULL, envelope bytea NOT NULL,
        PRIMARY KEY(tenant,event_id));
        CREATE TABLE IF NOT EXISTS raddle_example_quarantine (
        id uuid PRIMARY KEY, tenant text NOT NULL, wire bytea NOT NULL,
        classification text NOT NULL);",
    )
    .execute(pool)
    .await?;
    // A poison delivery must not prevent the later valid publications from completing.
    broker.inject_malformed_fixture().await?;
    let first: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp() - interval '1 second'")
        .fetch_one(pool)
        .await?;
    for (command, key) in plan.commands.iter().zip(&plan.keys) {
        let schedule = RecurringSchedule::new(
            key.clone(),
            first,
            Duration::from_secs(86400),
            MissedTickPolicy::SkipMissed,
            command,
            &[Delivery::new(DeliveryKind::Local, SUBSCRIPTION, first)],
        )?;
        let mut scope = store.begin().await?;
        scope.register_recurring(&schedule).await?;
        scope.register_recurring(&schedule).await?;
        scope.commit(Utc::now()).await?;
    }
    let policy = RetryPolicy::try_new(
        Duration::from_millis(10),
        Duration::from_millis(50),
        Duration::from_secs(5),
    )?;
    let mut worker = Worker::new(store.clone(), policy);
    worker.handle_saga::<PlaceOrder, OrderSaga, _, _>(
        SUBSCRIPTION,
        Codecs::default(),
        |command| {
            CorrelationKey::new(command.order_id.to_string()).map_err(|_| HandlerFailure::Reject)
        },
        SagaInitialization::Start(OrderSaga::default),
        |command, saga, scope| Box::pin(accept_order(command, saga, scope)),
    )?;
    let worker = Arc::new(worker);
    let publisher = broker
        .connection
        .publisher([Route {
            alias: DESTINATION.into(),
            exchange: broker.exchange.clone(),
            routing_key: "order.accepted".into(),
        }])
        .await?;
    let outbox = Arc::new(OutboxWorker::new(store.clone(), publisher, policy));
    let metrics = Arc::new(Metrics::default());
    let streams: Vec<_> = plan
        .commands
        .iter()
        .map(|command| EventStream::new(&plan.tenant, command.order_id, "example.order", 1))
        .collect::<std::result::Result<_, _>>()?;
    let mut host = Host::new(Duration::from_secs(5))?;
    let polling = PollSettings::new(16, Duration::from_millis(10))?;
    host.service("orders", RestartPolicy::NEVER, move |context| {
        let worker = Arc::clone(&worker);
        async move { raddle_host::postgres_worker(&worker, polling, context).await }
    })?;
    let publishing = Arc::clone(&outbox);
    host.service("outbox", RestartPolicy::NEVER, move |context| {
        let outbox = Arc::clone(&publishing);
        async move { raddle_host::postgres_outbox(&outbox, polling, context).await }
    })?;
    let scheduling_store = store.clone();
    let scheduling_metrics = Arc::clone(&metrics);
    host.service("scheduler", RestartPolicy::NEVER, move |context| {
        scheduler_service(
            scheduling_store.clone(),
            Arc::clone(&scheduling_metrics),
            context,
        )
    })?;
    let projection_store = store.clone();
    let projection_metrics = Arc::clone(&metrics);
    let projection_streams = streams.clone();
    host.service("projection", RestartPolicy::NEVER, move |context| {
        projection_service(
            projection_store.clone(),
            projection_streams.clone(),
            Arc::clone(&projection_metrics),
            context,
        )
    })?;
    let receiving_connection = Arc::clone(&broker.admin);
    let receiving_queue = broker.queue.clone();
    let receiving_tenant = plan.tenant.clone();
    let receiving_store = store.clone();
    let receiving_metrics = Arc::clone(&metrics);
    host.service("receiver", RestartPolicy::NEVER, move |context| {
        receiver_service(
            receiving_connection.clone(),
            receiving_queue.clone(),
            receiving_tenant.clone(),
            receiving_store.clone(),
            Arc::clone(&receiving_metrics),
            context,
        )
    })?;
    let handle = host.handle();
    ensure(
        !handle.snapshot().ready,
        "unstarted host falsely reported readiness",
    )?;
    let (host_result, observed) = tokio::join!(host.run(), async {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            observe(pool, &plan.tenant, &handle, &metrics),
        )
        .await;
        handle.shutdown();
        result.map_err(|_| "application completion deadline expired")?
    });
    // No publication future can still borrow the transport after the host joins.
    let publisher_release: Result<()> = match Arc::try_unwrap(outbox) {
        Ok(worker) => {
            *publisher_cleanup = Some(worker.into_transport());
            Ok(())
        }
        Err(_) => Err("joined host retained an outbox worker".into()),
    };
    host_result?;
    observed?;
    publisher_release?;
    ensure(
        !handle.snapshot().ready && !handle.snapshot().running,
        "host did not stop",
    )?;
    tokio::time::timeout(
        Duration::from_secs(5),
        verify(pool, store, plan, broker, &streams, &metrics),
    )
    .await
    .map_err(|_| "post-shutdown verification deadline expired")?
}

async fn accept_order(
    command: PlaceOrder,
    saga: &mut Saga<OrderSaga>,
    scope: &mut Scope<'static>,
) -> std::result::Result<(), HandlerFailure> {
    let tenant = scope.tenant().ok_or(HandlerFailure::Reject)?.to_owned();
    let incoming = scope
        .envelope()
        .map_err(|_| HandlerFailure::Retry)?
        .ok_or(HandlerFailure::Reject)?;
    let event = Envelope::with_identity(
        command.event_id,
        incoming.created_at(),
        &OrderAccepted {
            tenant: tenant.clone(),
            order_id: command.order_id,
            units: command.units,
            command_id: incoming.id(),
        },
    )
    .and_then(|event| {
        event.correlated(
            incoming.correlation_id().unwrap_or(incoming.id()),
            incoming.id(),
        )
    })
    .map_err(|_| HandlerFailure::Reject)?;
    sqlx::query("INSERT INTO raddle_example_orders(tenant,order_id,units,command_id,event_id) VALUES($1,$2,$3,$4,$5)")
        .bind(&tenant).bind(command.order_id).bind(command.units).bind(incoming.id()).bind(event.id())
        .execute(scope.connection()).await.map_err(|_| HandlerFailure::Retry)?;
    let stream = EventStream::new(&tenant, command.order_id, "example.order", 1)
        .map_err(|_| HandlerFailure::Reject)?;
    scope
        .append_events(
            &stream,
            ExpectedRevision::NoStream,
            std::slice::from_ref(&event),
        )
        .await
        .map_err(|_| HandlerFailure::Retry)?;
    scope
        .stage(
            SOURCE,
            &tenant,
            &event,
            &[Delivery::new(
                DeliveryKind::Remote,
                DESTINATION,
                incoming.created_at(),
            )],
        )
        .await
        .map_err(|_| HandlerFailure::Retry)?;
    saga.state_mut().accepted_units = command.units;
    saga.finish();
    Ok(())
}

async fn scheduler_service(
    store: Store,
    metrics: Arc<Metrics>,
    mut context: ServiceContext,
) -> std::result::Result<(), ServiceFailure> {
    let scheduler = RecurringScheduler::new(store);
    loop {
        if *context.shutdown().borrow() {
            return Ok(());
        }
        let page = scheduler
            .run_once(16, 16)
            .await
            .map_err(|_| ServiceFailure::Transient)?;
        metrics
            .scheduled
            .fetch_add(page.occurrences as usize, Ordering::SeqCst);
        // Schedule-local business failures remain recorded/retryable and do not stop siblings.
        metrics
            .schedule_failures
            .fetch_add(page.failures.len(), Ordering::SeqCst);
        context.set_ready(true);
        tokio::select! {
            biased;
            _ = context.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

async fn projection_service(
    store: Store,
    streams: Vec<EventStream>,
    metrics: Arc<Metrics>,
    mut context: ServiceContext,
) -> std::result::Result<(), ServiceFailure> {
    loop {
        if *context.shutdown().borrow() {
            return Ok(());
        }
        let mut healthy = true;
        for stream in &streams {
            // Each stream/checkpoint has its own scope, so one stream cannot roll back another.
            if project(&store, stream).await.is_err() {
                metrics.projection_failures.fetch_add(1, Ordering::SeqCst);
                healthy = false;
            }
        }
        context.set_ready(healthy);
        tokio::select! {
            biased;
            _ = context.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

async fn project(
    store: &Store,
    stream: &EventStream,
) -> std::result::Result<ProjectionProgress, EventError> {
    store.project_stream(PROJECTION, stream, 16, |recorded, scope| Box::pin(async move {
        let event = recorded.envelope.decode::<OrderAccepted>()?;
        if event.tenant != recorded.stream.tenant() || event.order_id != recorded.stream.id() {
            return Err(EventError::Rejected("event ownership mismatch"));
        }
        sqlx::query("INSERT INTO raddle_example_order_view(tenant,order_id,units,event_id,apply_count) VALUES($1,$2,$3,$4,1)
            ON CONFLICT(tenant,order_id) DO UPDATE SET units=excluded.units,event_id=excluded.event_id,apply_count=raddle_example_order_view.apply_count+1")
            .bind(&event.tenant).bind(event.order_id).bind(event.units).bind(recorded.envelope.id())
            .execute(scope.connection()).await?;
        Ok(())
    })).await
}

async fn receiver_service(
    connection: Arc<AmqpConnection>,
    queue: String,
    tenant: String,
    store: Store,
    metrics: Arc<Metrics>,
    mut context: ServiceContext,
) -> std::result::Result<(), ServiceFailure> {
    let channel = connection
        .create_channel()
        .await
        .map_err(|_| ServiceFailure::Transient)?;
    let result = async {
        loop {
            if *context.shutdown().borrow() {
                return Ok(());
            }
            // Do not cancel a delivered acknowledgement halfway through cooperative shutdown.
            // Basic.Get owns one delivery at a time and retains raw malformed wire
            // for durable quarantine. No delegate callback or detached task exists.
            if let Some(delivery) = channel
                .basic_get(queue.clone().into(), BasicGetOptions::default())
                .await
                .map_err(|_| ServiceFailure::Transient)?
            {
                accept_delivery(&store, &tenant, delivery.delivery, &metrics).await?;
            }
            context.set_ready(true); // An actual broker round trip and callback completed.
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }
    .await;
    let closed = bounded_cleanup("receiver channel", async {
        channel.close(200, "receiver joined".into()).await?;
        Ok(())
    })
    .await
    .map_err(|_| ServiceFailure::Transient);
    result.and(closed)
}

async fn accept_delivery(
    store: &Store,
    tenant: &str,
    delivery: BrokerDelivery,
    metrics: &Metrics,
) -> std::result::Result<(), ServiceFailure> {
    let decoded = validated_delivery(&delivery).and_then(|(envelope, bytes)| {
        envelope
            .decode::<OrderAccepted>()
            .map(|event| (envelope, bytes, event))
    });
    let (envelope, bytes, event) = match decoded {
        Ok((envelope, bytes, event))
            if event.tenant == tenant && (1..=1000).contains(&event.units) =>
        {
            (envelope, bytes, event)
        }
        _ => return quarantine(store, tenant, &delivery, "invalid-wire-or-owner", metrics).await,
    };
    let mut scope = store.begin().await.map_err(|_| ServiceFailure::Transient)?;
    sqlx::query("INSERT INTO raddle_example_receipts(tenant,event_id,envelope) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(&event.tenant).bind(envelope.id()).bind(bytes).execute(scope.connection()).await.map_err(|_| ServiceFailure::Transient)?;
    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT envelope FROM raddle_example_receipts WHERE tenant=$1 AND event_id=$2",
    )
    .bind(tenant)
    .bind(envelope.id())
    .fetch_one(scope.connection())
    .await
    .map_err(|_| ServiceFailure::Transient)?;
    if stored != bytes {
        scope
            .rollback()
            .await
            .map_err(|_| ServiceFailure::Transient)?;
        return quarantine(
            store,
            tenant,
            &delivery,
            "conflicting-receipt-identity",
            metrics,
        )
        .await;
    }
    scope
        .commit(Utc::now())
        .await
        .map_err(|_| ServiceFailure::Transient)?;
    // Stable receipt identity makes the commit/ack uncertainty window idempotent.
    acknowledge(&delivery).await?;
    metrics.settled.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn validated_delivery(
    delivery: &BrokerDelivery,
) -> std::result::Result<(Envelope, &[u8]), WireError> {
    let envelope = Envelope::from_slice(&delivery.data, Default::default())?;
    Ok((envelope, &delivery.data))
}

async fn quarantine(
    store: &Store,
    tenant: &str,
    delivery: &BrokerDelivery,
    classification: &'static str,
    metrics: &Metrics,
) -> std::result::Result<(), ServiceFailure> {
    let mut scope = store.begin().await.map_err(|_| ServiceFailure::Transient)?;
    sqlx::query(
        "INSERT INTO raddle_example_quarantine(id,tenant,wire,classification) VALUES($1,$2,$3,$4)",
    )
    .bind(Uuid::new_v4())
    .bind(tenant)
    .bind(&delivery.data)
    .bind(classification)
    .execute(scope.connection())
    .await
    .map_err(|_| ServiceFailure::Transient)?;
    scope
        .commit(Utc::now())
        .await
        .map_err(|_| ServiceFailure::Transient)?;
    acknowledge(delivery).await?;
    metrics.quarantined.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

async fn acknowledge(delivery: &BrokerDelivery) -> std::result::Result<(), ServiceFailure> {
    let sent = delivery
        .ack(BasicAckOptions::default())
        .await
        .map_err(|_| ServiceFailure::Transient)?;
    if sent {
        Ok(())
    } else {
        Err(ServiceFailure::Transient)
    }
}

async fn observe(
    pool: &PgPool,
    tenant: &str,
    handle: &HostHandle,
    metrics: &Metrics,
) -> Result<()> {
    loop {
        let counts: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT
            (SELECT count(*) FROM raddle_example_orders WHERE tenant=$1),
            (SELECT count(*) FROM raddle_example_order_view WHERE tenant=$1),
            (SELECT count(*) FROM raddle_example_receipts WHERE tenant=$1),
            (SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND status='completed'),
            (SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND status<>'completed'),
            (SELECT count(*) FROM raddle_example_quarantine WHERE tenant=$1)",
        )
        .bind(tenant)
        .fetch_one(pool)
        .await?;
        let snapshot = handle.snapshot();
        if snapshot.ready
            && counts == (2, 2, 2, 4, 0, 1)
            && metrics.settled.load(Ordering::SeqCst) == 2
            && metrics.quarantined.load(Ordering::SeqCst) == 1
        {
            return Ok(());
        }
        ensure(
            snapshot.running,
            "a hosted service failed before application completion",
        )?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn verify(
    pool: &PgPool,
    store: &Store,
    plan: &Plan,
    broker: &Broker,
    streams: &[EventStream],
    metrics: &Metrics,
) -> Result<Summary> {
    ensure(
        metrics.schedule_failures.load(Ordering::SeqCst) == 0
            && metrics.projection_failures.load(Ordering::SeqCst) == 0
            && metrics.quarantined.load(Ordering::SeqCst) == 1,
        "an application component reported an isolated failure",
    )?;
    ensure(
        metrics.scheduled.load(Ordering::SeqCst) == 2,
        "schedule emitted duplicate or missing occurrences",
    )?;
    for (command, stream) in plan.commands.iter().zip(streams) {
        let row = sqlx::query("SELECT o.units,o.command_id,o.event_id,v.units AS projected_units,v.apply_count,
            c.revision,s.completed,s.state->>'accepted_units' AS saga_units
            FROM raddle_example_orders o JOIN raddle_example_order_view v USING(tenant,order_id)
            JOIN raddle_event_checkpoints c ON c.tenant=o.tenant AND c.stream_id=o.order_id AND c.projection=$3
            JOIN raddle_sagas s ON s.tenant=o.tenant AND s.correlation=o.order_id::text AND s.kind=$4
            WHERE o.tenant=$1 AND o.order_id=$2")
            .bind(&plan.tenant).bind(command.order_id).bind(PROJECTION).bind(OrderSaga::NAME).fetch_one(pool).await?;
        let command_id: Uuid = row.try_get("command_id")?;
        ensure(
            row.try_get::<i32, _>("units")? == command.units
                && row.try_get::<i32, _>("projected_units")? == command.units
                && row.try_get::<i32, _>("apply_count")? == 1
                && row.try_get::<i64, _>("revision")? == 1
                && row.try_get::<bool, _>("completed")?
                && row.try_get::<String, _>("saga_units")? == command.units.to_string()
                && row.try_get::<Uuid, _>("event_id")? == command.event_id,
            "business/saga/projection state differs",
        )?;
        let local: Vec<u8> = sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE tenant=$1 AND message_id=$2 AND kind='local' AND status='completed'")
            .bind(&plan.tenant).bind(command_id).fetch_one(pool).await?;
        let incoming = Envelope::from_slice(&local, Default::default())?;
        ensure(
            incoming.id() == command_id
                && incoming.decode::<PlaceOrder>()?.tenant_hint == FORGED_TENANT,
            "scheduled typed command identity was not retained",
        )?;
        let event: Vec<u8> = sqlx::query_scalar(
            "SELECT envelope FROM raddle_events WHERE tenant=$1 AND stream_id=$2 AND revision=1",
        )
        .bind(&plan.tenant)
        .bind(command.order_id)
        .fetch_one(pool)
        .await?;
        let outgoing: Vec<u8> = sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE tenant=$1 AND message_id=$2 AND kind='remote' AND status='completed'")
            .bind(&plan.tenant).bind(command.event_id).fetch_one(pool).await?;
        let received: Vec<u8> = sqlx::query_scalar(
            "SELECT envelope FROM raddle_example_receipts WHERE tenant=$1 AND event_id=$2",
        )
        .bind(&plan.tenant)
        .bind(command.event_id)
        .fetch_one(pool)
        .await?;
        let envelope = Envelope::from_slice(&event, Default::default())?;
        ensure(
            event == outgoing
                && outgoing == received
                && envelope.id() == command.event_id
                && envelope.causation_id() == Some(command_id)
                && envelope.correlation_id() == Some(command_id)
                && envelope.decode::<OrderAccepted>()?.command_id == command_id,
            "wire identity/content changed between event, outbox, and broker",
        )?;
        let replay = project(store, stream).await?;
        ensure(
            replay.applied == 0 && replay.previous_revision == 1 && replay.revision == 1,
            "projection reapplied completed effects",
        )?;
    }
    let forbidden: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_sagas WHERE tenant=$1")
        .bind(FORGED_TENANT)
        .fetch_one(pool)
        .await?;
    let rejected: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_example_orders WHERE tenant=$1 AND order_id=$2",
    )
    .bind(&plan.tenant)
    .bind(plan.rejected_order)
    .fetch_one(pool)
    .await?;
    ensure(
        forbidden == 0 && rejected == 0,
        "untrusted tenant or invalid command reached business state",
    )?;
    let mut receiver = broker.connection.receiver(&broker.queue, 1).await?;
    let queued = receiver.receive(Duration::from_millis(100)).await;
    let closed = bounded_cleanup("verification receiver", async {
        receiver.close().await?;
        Ok(())
    })
    .await;
    ensure(
        queued?.is_none(),
        "a settled broker message remained queued",
    )?;
    closed?;
    Ok(Summary {
        tenant: plan.tenant.clone(),
        accepted: 2,
        rejected: 1,
        scheduled: metrics.scheduled.load(Ordering::SeqCst),
        completed_jobs: 4,
        projected: 2,
        settled: metrics.settled.load(Ordering::SeqCst),
        quarantined: metrics.quarantined.load(Ordering::SeqCst),
        event_ids: plan.commands.iter().map(|c| c.event_id).collect(),
        rejected_order: plan.rejected_order,
    })
}

async fn cancel_schedule(store: &Store, key: &ScheduleKey) -> Result<()> {
    let mut scope = store.begin().await?;
    scope.cancel_recurring(key).await?;
    scope.commit(Utc::now()).await?;
    Ok(())
}

async fn bounded_cleanup(
    name: &'static str,
    future: impl std::future::Future<Output = Result<()>> + Send,
) -> Result<()> {
    Budget::new(Duration::from_secs(2), Duration::from_secs(2))?
        .run(vec![Operation::new(name, future)])
        .await
}

struct Broker {
    connection: Connection,
    admin: Arc<AmqpConnection>,
    exchange: String,
    queue: String,
}
impl Broker {
    async fn connect(uri: &str) -> Result<Self> {
        let connection = Connection::connect(uri).await?;
        let admin = match AmqpConnection::connect(uri, ConnectionProperties::default()).await {
            Ok(admin) => admin,
            Err(error) => {
                let _ = bounded_cleanup("partial broker connection", async {
                    connection.close().await?;
                    Ok(())
                })
                .await;
                return Err(error.into());
            }
        };
        let name = format!("raddle-application-{}", Uuid::new_v4());
        Ok(Self {
            connection,
            admin: Arc::new(admin),
            exchange: format!("{name}-exchange"),
            queue: format!("{name}-queue"),
        })
    }
    async fn declare(&self) -> Result<()> {
        let topology = Topology::new(
            [Exchange {
                name: self.exchange.clone(),
                kind: ExchangeKind::Topic,
                durable: true,
            }],
            [Queue {
                name: self.queue.clone(),
                durable: true,
            }],
            [Binding {
                queue: self.queue.clone(),
                exchange: self.exchange.clone(),
                routing_key: "order.*".into(),
            }],
        )?;
        self.connection.declare_topology(&topology).await?;
        Ok(())
    }
    async fn inject_malformed_fixture(&self) -> Result<()> {
        let channel = self.admin.create_channel().await?;
        let result = async {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            let confirmed = channel
                .basic_publish(
                    "".into(),
                    self.queue.clone().into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    b"application fixture: malformed envelope",
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await?
                .await?;
            ensure(
                matches!(confirmed, Confirmation::Ack(None)),
                "malformed fixture was not routed and confirmed",
            )
        }
        .await;
        let closed = bounded_cleanup("fixture channel", async {
            channel.close(200, "fixture sent".into()).await?;
            Ok(())
        })
        .await;
        result?;
        closed?;
        Ok(())
    }
    async fn close(
        self,
        store: &Store,
        keys: &[ScheduleKey],
        publisher: Option<Publisher>,
    ) -> Result<()> {
        let budget = Budget::new(Duration::from_secs(5), Duration::from_secs(2))?;
        let mut independent = Vec::new();
        if let Some(publisher) = publisher {
            independent.push(Operation::new("publisher channel", async {
                publisher.close().await?;
                Ok(())
            }));
        }
        for key in keys {
            independent.push(Operation::new(
                "cancel recurring schedule",
                cancel_schedule(store, key),
            ));
        }
        // Deletion channels are independent. Neither a PostgreSQL row lock nor
        // one broker channel failure can prevent the remaining attempts.
        independent.push(Operation::new("delete owned queue", async {
            let channel = self.admin.create_channel().await?;
            channel
                .queue_delete(self.queue.clone().into(), QueueDeleteOptions::default())
                .await?;
            channel.close(200, "queue removed".into()).await?;
            Ok(())
        }));
        independent.push(Operation::new("delete owned exchange", async {
            let channel = self.admin.create_channel().await?;
            channel
                .exchange_delete(
                    self.exchange.clone().into(),
                    ExchangeDeleteOptions::default(),
                )
                .await?;
            channel.close(200, "exchange removed".into()).await?;
            Ok(())
        }));
        let first_phase = budget.run(independent).await;
        // Reuse the original outer deadline; connection close gets no new budget.
        let second_phase = budget
            .run(vec![
                Operation::new("adapter connection", async {
                    self.connection.close().await?;
                    Ok(())
                }),
                Operation::new("admin connection", async {
                    self.admin.close(200, "example finished".into()).await?;
                    Ok(())
                }),
            ])
            .await;
        first_phase?;
        second_phase?;
        Ok(())
    }
}

fn ensure(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod regression_tests {
    use chrono::Utc;
    use lapin::message::Delivery;
    use raddle::wire::Envelope;
    use uuid::Uuid;

    use super::{OrderAccepted, validated_delivery};

    #[test]
    fn validated_receipt_retains_original_noncanonical_wire_bytes() {
        let encoded = Envelope::with_identity(
            Uuid::new_v4(),
            Utc::now(),
            &OrderAccepted {
                tenant: "receipt-test".into(),
                order_id: Uuid::new_v4(),
                units: 1,
                command_id: Uuid::new_v4(),
            },
        )
        .unwrap()
        .to_vec()
        .unwrap();
        let mut wire = b" \n".to_vec();
        wire.extend_from_slice(&encoded);
        wire.extend_from_slice(b"\t ");
        let delivery = Delivery::mock(1, "test".into(), "test".into(), false, wire.clone());
        let (envelope, receipt) = validated_delivery(&delivery).unwrap();
        assert_ne!(envelope.to_vec().unwrap(), wire);
        assert_eq!(
            receipt, wire,
            "receipt normalized the broker's original bytes"
        );
    }

    #[test]
    fn receipt_validation_still_rejects_invalid_and_oversized_original_wire() {
        for bytes in [b"not an envelope".to_vec(), vec![b' '; 1_048_577]] {
            let delivery = Delivery::mock(1, "test".into(), "test".into(), false, bytes);
            assert!(validated_delivery(&delivery).is_err());
        }
    }
}
