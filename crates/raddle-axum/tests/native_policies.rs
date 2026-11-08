//! Output caching, route rate limits, session CSRF and shared core admission.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use raddle::{Request, RuntimeBuilder};
use raddle_axum::native::{
    CacheEligibility, CookieSessions, CsrfPolicy, Endpoint, EndpointContext, EndpointContract,
    EndpointOutput, EndpointRegistration, EndpointResponse, HttpMethod, NativeApiBuilder,
    OutputCache, OutputCachePolicy, Problem, RateAlgorithm, RateLimiter, RatePolicy, RequestInput,
    ResponseCache,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use serde_json::{Value, json};
use tokio::sync::Notify;

fn auth(headers: &HeaderMap) -> Result<Principal, ApiError> {
    match headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
    {
        Some("Bearer allowed") => Principal::try_new("tenant-a", "alice", [Permission::Invoke]),
        Some("Bearer other") => Principal::try_new("tenant-b", "bob", [Permission::Invoke]),
        _ => Err(ApiError::unauthorized()),
    }
}

#[derive(Clone)]
struct Counters {
    authorize: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    allowed: Arc<AtomicBool>,
}
impl Counters {
    fn new() -> Self {
        Self {
            authorize: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
            allowed: Arc::new(AtomicBool::new(true)),
        }
    }
}
struct Read<const N: usize> {
    counts: Counters,
    delay: Duration,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}
impl<const N: usize> Read<N> {
    fn new(counts: Counters) -> Self {
        Self {
            counts,
            delay: Duration::ZERO,
            entered: None,
            release: None,
        }
    }
}
impl<const N: usize> Endpoint for Read<N> {
    type Input = String;
    type Resource = Value;
    type Error = Infallible;
    async fn before_bind(&self, _input: &mut RequestInput) -> Result<(), Problem> {
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        if let Some(release) = &self.release {
            release.notified().await;
        }
        Ok(())
    }
    async fn bind(&self, input: &mut RequestInput) -> Result<String, Problem> {
        input.parameters.query("q", String::new())
    }
    async fn authorize(
        &self,
        _context: &mut EndpointContext,
        _input: &String,
    ) -> Result<(), Problem> {
        self.counts.authorize.fetch_add(1, Ordering::SeqCst);
        if !self.counts.allowed.load(Ordering::SeqCst) {
            return Err(Problem::new(StatusCode::FORBIDDEN));
        }
        Ok(())
    }
    async fn handle(
        &self,
        context: &mut EndpointContext,
        input: String,
    ) -> Result<EndpointOutput<Value>, Infallible> {
        let call = self.counts.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(EndpointResponse::Json(json!({"call":call,"q":input,"tenant":context.principal().map(Principal::tenant),"subject":context.principal().map(Principal::subject)})).into())
    }
}

async fn serve(api: &Api) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}

#[tokio::test]
async fn output_cache_varies_expires_evicts_singleflights_and_always_authorizes() {
    let counts = Counters::new();
    let cache = OutputCache::new(16, 4096).unwrap();
    let mut runtime = RuntimeBuilder::default();
    let mut endpoint = Read::<0>::new(counts.clone());
    endpoint.delay = Duration::from_millis(30);
    let mut contract = EndpointContract::new(HttpMethod::Get, "/cached", "cached");
    contract.output_cache = Some(OutputCachePolicy {
        store: cache.clone(),
        ttl: Duration::from_secs(5),
        vary_query: vec!["q".into()],
        vary_headers: vec!["x-format".into()],
        tags: vec!["items".into()],
        eligibility: CacheEligibility::AuthenticatedPartition,
    });
    contract.response_cache = Some(ResponseCache {
        max_age: Some(Duration::from_secs(12)),
        public: false,
        vary: vec!["X-Format".into()],
    });
    let reg = EndpointRegistration::prepare(&mut runtime, contract, endpoint).unwrap();
    let mut default = EndpointContract::new(HttpMethod::Get, "/default", "default");
    default.output_cache = Some(OutputCachePolicy {
        store: cache.clone(),
        ttl: Duration::from_secs(1),
        vary_query: vec![],
        vary_headers: vec![],
        tags: vec![],
        eligibility: CacheEligibility::Anonymous,
    });
    let default =
        EndpointRegistration::prepare(&mut runtime, default, Read::<1>::new(counts.clone()))
            .unwrap();
    let mut short = EndpointContract::new(HttpMethod::Get, "/short", "short");
    short.output_cache = Some(OutputCachePolicy {
        store: cache.clone(),
        ttl: Duration::from_millis(50),
        vary_query: vec![],
        vary_headers: vec![],
        tags: vec![],
        eligibility: CacheEligibility::AuthenticatedPartition,
    });
    let short =
        EndpointRegistration::prepare(&mut runtime, short, Read::<2>::new(counts.clone())).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder
        .endpoint(reg)
        .unwrap()
        .endpoint(default)
        .unwrap()
        .endpoint(short)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    let (one, two) = tokio::join!(
        client
            .get(format!("{url}/cached?q=a"))
            .bearer_auth("allowed")
            .send(),
        client
            .get(format!("{url}/cached?q=a"))
            .bearer_auth("allowed")
            .send()
    );
    let one = one.unwrap();
    assert_eq!(one.headers()["cache-control"], "private, max-age=12");
    assert_eq!(one.headers()["vary"], "X-Format");
    assert_eq!(
        one.json::<Value>().await.unwrap(),
        two.unwrap().json::<Value>().await.unwrap()
    );
    assert_eq!(counts.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.authorize.load(Ordering::SeqCst), 2);
    counts.allowed.store(false, Ordering::SeqCst);
    assert_eq!(
        client
            .get(format!("{url}/cached?q=a"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(counts.calls.load(Ordering::SeqCst), 1);
    counts.allowed.store(true, Ordering::SeqCst);
    for (query, format, identity) in [
        ("b", "one", "allowed"),
        ("a", "two", "allowed"),
        ("a", "one", "other"),
    ] {
        assert_eq!(
            client
                .get(format!("{url}/cached?q={query}"))
                .bearer_auth(identity)
                .header("x-format", format)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 4);
    assert_eq!(cache.evict_tag("unrelated").unwrap(), 0);
    assert_eq!(cache.evict_tag("items").unwrap(), 4);
    client
        .get(format!("{url}/cached?q=a"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap();
    assert_eq!(counts.calls.load(Ordering::SeqCst), 5);
    for _ in 0..2 {
        client
            .get(format!("{url}/default"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap();
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 7);
    client
        .get(format!("{url}/short"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    client
        .get(format!("{url}/short"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap();
    assert_eq!(counts.calls.load(Ordering::SeqCst), 9);
    assert!(api.specification()["paths"]["/cached"]["get"]["x-response-cache"].is_object());
    assert_eq!(
        api.specification()["paths"]["/cached"]["get"]["responses"]["200"]["headers"]["Cache-Control"]
            ["schema"]["type"],
        "string"
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn cookie_session_csrf_is_opt_in_enforced_and_revocation_precedes_cached_hits() {
    let sessions = CookieSessions::new(4, false).unwrap();
    let issued = sessions
        .issue(
            Principal::try_new("tenant-a", "alice", [Permission::Invoke]).unwrap(),
            Duration::from_secs(60),
        )
        .unwrap();
    let cookie = issued.set_cookie.split(';').next().unwrap();
    assert!(!format!("{issued:?}").contains(&issued.csrf_token));
    let counts = Counters::new();
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/protected", "protected");
    contract.csrf = CsrfPolicy::Session(sessions.clone());
    let protected =
        EndpointRegistration::prepare(&mut runtime, contract, Read::<10>::new(counts.clone()))
            .unwrap();
    let unprotected = EndpointRegistration::prepare(
        &mut runtime,
        EndpointContract::new(HttpMethod::Post, "/unprotected", "unprotected"),
        Read::<11>::new(counts.clone()),
    )
    .unwrap();
    let mut cached = EndpointContract::new(HttpMethod::Get, "/cached", "cached");
    cached.output_cache = Some(OutputCachePolicy {
        store: OutputCache::new(2, 4096).unwrap(),
        ttl: Duration::from_secs(10),
        vary_query: vec![],
        vary_headers: vec![],
        tags: vec![],
        eligibility: CacheEligibility::AuthenticatedPartition,
    });
    let cached =
        EndpointRegistration::prepare(&mut runtime, cached, Read::<12>::new(counts.clone()))
            .unwrap();
    let mut builder = NativeApiBuilder::cookie_sessions(runtime.build(), sessions.clone());
    builder
        .endpoint(protected)
        .unwrap()
        .endpoint(unprotected)
        .unwrap()
        .endpoint(cached)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for token in [None, Some("wrong")] {
        let mut request = client
            .post(format!("{url}/protected"))
            .header("cookie", cookie);
        if let Some(token) = token {
            request = request.header("x-csrf-token", token);
        }
        assert_eq!(request.send().await.unwrap().status(), 400);
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client
            .post(format!("{url}/protected"))
            .header("cookie", cookie)
            .header("x-csrf-token", &issued.csrf_token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{url}/unprotected"))
            .header("cookie", cookie)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{url}/cached"))
                .header("cookie", cookie)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 3);
    let mut headers = HeaderMap::new();
    headers.insert("cookie", cookie.parse().unwrap());
    assert!(sessions.revoke(&headers).unwrap());
    assert_eq!(
        client
            .get(format!("{url}/cached"))
            .header("cookie", cookie)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(counts.calls.load(Ordering::SeqCst), 3);
    task.abort();
    let _ = task.await;
}

async fn exercise_rate<const N: usize>(algorithm: RateAlgorithm, concurrent: bool) {
    let counts = Counters::new();
    let mut runtime = RuntimeBuilder::default();
    let mut endpoint = Read::<N>::new(counts.clone());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    if concurrent {
        endpoint.entered = Some(Arc::clone(&entered));
        endpoint.release = Some(Arc::clone(&release));
    }
    let mut contract = EndpointContract::new(HttpMethod::Get, "/limited", "limited");
    contract.rate = Some(
        RatePolicy::new(
            "read",
            RateLimiter::new(algorithm, 1, Duration::from_secs(60)).unwrap(),
        )
        .unwrap(),
    );
    let reg = EndpointRegistration::prepare(&mut runtime, contract, endpoint).unwrap();
    let exempt = EndpointRegistration::prepare(
        &mut runtime,
        EndpointContract::new(HttpMethod::Get, "/exempt", "exempt"),
        Read::<100>::new(counts.clone()),
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap().endpoint(exempt).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{url}/limited"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    if concurrent {
        let first_client = client.clone();
        let first_url = url.clone();
        let first = tokio::spawn(async move {
            first_client
                .get(format!("{first_url}/limited"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
        });
        entered.notified().await;
        assert_eq!(
            client
                .get(format!("{url}/limited"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            429
        );
        release.notify_one();
        assert_eq!(first.await.unwrap().status(), 200);
    } else {
        assert_eq!(
            client
                .get(format!("{url}/limited"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .get(format!("{url}/limited"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            429
        );
    }
    assert_eq!(
        client
            .get(format!("{url}/limited"))
            .bearer_auth("other")
            .send()
            .await
            .unwrap()
            .status(),
        429,
        "finite identity table must not silently evict live quotas"
    );
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{url}/exempt"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    if concurrent {
        release.notify_one();
        assert_eq!(
            client
                .get(format!("{url}/limited"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn all_four_named_rate_algorithms_enforce_wire_bounds_and_route_exemption() {
    exercise_rate::<20>(
        RateAlgorithm::Fixed {
            permits: 1,
            window: Duration::from_secs(30),
        },
        false,
    )
    .await;
    exercise_rate::<21>(
        RateAlgorithm::Sliding {
            permits: 1,
            window: Duration::from_secs(30),
        },
        false,
    )
    .await;
    exercise_rate::<22>(
        RateAlgorithm::TokenBucket {
            capacity: 1,
            refill: 1,
            period: Duration::from_secs(30),
        },
        false,
    )
    .await;
    exercise_rate::<23>(RateAlgorithm::Concurrency { permits: 1 }, true).await;
}

#[tokio::test]
async fn anonymous_cache_eligibility_and_saturation_never_change_live_responses() {
    let counts = Counters::new();
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Get, "/anonymous", "anonymous");
    contract.anonymous = true;
    contract.output_cache = Some(OutputCachePolicy {
        store: OutputCache::new(1, 256).unwrap(),
        ttl: Duration::from_secs(60),
        vary_query: vec!["q".into()],
        vary_headers: vec![],
        tags: vec![],
        eligibility: CacheEligibility::Anonymous,
    });
    let reg =
        EndpointRegistration::prepare(&mut runtime, contract, Read::<40>::new(counts.clone()))
            .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{url}/anonymous?q=a"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{url}/anonymous?q=a"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(counts.calls.load(Ordering::SeqCst), 3);
    for _ in 0..2 {
        assert_eq!(
            client
                .get(format!("{url}/anonymous?q=b"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(
        counts.calls.load(Ordering::SeqCst),
        5,
        "full cache bypasses rather than replacing live output"
    );
    task.abort();
    let _ = task.await;
}

struct Other;
impl Request for Other {
    type Reply = ();
    type Error = Infallible;
}

#[tokio::test]
async fn native_middleware_holds_the_existing_core_runtime_permit() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let other_entered = Arc::new(Notify::new());
    let mut runtime = RuntimeBuilder::try_new(1).unwrap();
    let mut endpoint = Read::<30>::new(Counters::new());
    endpoint.entered = Some(Arc::clone(&entered));
    endpoint.release = Some(Arc::clone(&release));
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        EndpointContract::new(HttpMethod::Get, "/middleware", "middleware"),
        endpoint,
    )
    .unwrap();
    let notify = Arc::clone(&other_entered);
    runtime
        .handle(move |_: Other| {
            let notify = Arc::clone(&notify);
            async move {
                notify.notify_one();
                Ok(())
            }
        })
        .unwrap();
    let runtime = runtime.build();
    let mut builder = NativeApiBuilder::new(runtime.clone(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!("{url}/middleware"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
    });
    entered.notified().await;
    let ordinary = tokio::spawn(async move { runtime.invoke(Other).await.unwrap() });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), other_entered.notified())
            .await
            .is_err()
    );
    release.notify_one();
    assert_eq!(request.await.unwrap().status(), 200);
    ordinary.await.unwrap();
    other_entered.notified().await;
    task.abort();
    let _ = task.await;
}

struct FinallyLimited {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}
impl Endpoint for FinallyLimited {
    type Input = ();
    type Resource = Value;
    type Error = Infallible;
    async fn bind(&self, _input: &mut RequestInput) -> Result<(), Problem> {
        Ok(())
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        _input: (),
    ) -> Result<EndpointOutput<Value>, Infallible> {
        Ok(EndpointResponse::Json(json!(true)).into())
    }
    async fn finally(&self, _context: &mut EndpointContext, status: StatusCode) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if status.is_success() {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[tokio::test]
async fn concurrency_policy_holds_its_permit_through_finally_not_just_the_handler() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Get, "/finally", "finally");
    contract.rate = Some(
        RatePolicy::new(
            "finally",
            RateLimiter::new(
                RateAlgorithm::Concurrency { permits: 1 },
                4,
                Duration::from_secs(1),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        contract,
        FinallyLimited {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
        },
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let first_url = url.clone();
    let first = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!("{first_url}/finally"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
    });
    entered.notified().await;
    let client = reqwest::Client::new();
    let second = tokio::time::timeout(
        Duration::from_secs(3),
        client
            .get(format!("{url}/finally"))
            .bearer_auth("allowed")
            .send(),
    )
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "rate-denied requests must not run endpoint callbacks"
    );
    release.notify_waiters();
    assert_eq!(first.await.unwrap().status(), 200);
    assert_eq!(second.unwrap().unwrap().status(), 429);
    release.notify_one();
    assert_eq!(
        client
            .get(format!("{url}/finally"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    task.abort();
    let _ = task.await;
}
