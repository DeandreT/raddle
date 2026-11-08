//! Buffered form/file contracts and genuinely incremental shared multipart reading.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::http::{HeaderMap, StatusCode};
use futures_util::stream;
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, MultipartLimits, NativeApiBuilder, Problem, RequestInput,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use serde_json::{Value, json};
use tokio::sync::Notify;

fn auth(_headers: &HeaderMap) -> Result<Principal, ApiError> {
    Principal::try_new("verified-tenant", "alice", [Permission::Invoke])
}

struct Form;
impl Endpoint for Form {
    type Input = Value;
    type Resource = Value;
    type Error = Infallible;
    async fn before_bind(&self, input: &mut RequestInput) -> Result<(), Problem> {
        let _ = input.form().await?;
        Ok(())
    }
    async fn bind(&self, input: &mut RequestInput) -> Result<Value, Problem> {
        let form = input.form().await?;
        let files: Vec<_> = form
            .files("upload")
            .map(|file| json!({"filename":file.filename,"bytes":file.bytes.to_vec()}))
            .collect();
        Ok(
            json!({"count":form.value("aliased-count",9u32)?,"metadata":form.values.get("reserved-name"),"files":files}),
        )
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: Value,
    ) -> Result<EndpointOutput<Value>, Infallible> {
        Ok(EndpointResponse::Json(input).into())
    }
    fn body_schema(&self) -> Value {
        json!({"type":"object","properties":{"aliased-count":{"type":"integer","default":9},"reserved-name":{"type":"string"},"upload":{"type":"array","items":{"type":"string","format":"binary"}}}})
    }
}

struct StreamUpload {
    first_consumed: Arc<Notify>,
    first_count: Arc<AtomicUsize>,
}
impl Endpoint for StreamUpload {
    type Input = Value;
    type Resource = Value;
    type Error = Infallible;
    async fn before_bind(&self, input: &mut RequestInput) -> Result<(), Problem> {
        let reader = input.multipart()?;
        let field = reader
            .next_field()
            .await?
            .ok_or_else(|| Problem::invalid("metadata"))?;
        assert_eq!(field.name(), Some("metadata"));
        let metadata = field.text().await.map_err(upload_error)?;
        if metadata != "first" {
            return Err(Problem::invalid("metadata"));
        }
        self.first_count.fetch_add(1, Ordering::SeqCst);
        self.first_consumed.notify_one();
        Ok(())
    }
    async fn bind(&self, input: &mut RequestInput) -> Result<Value, Problem> {
        let reader = input.multipart()?;
        let mut field = reader
            .next_field()
            .await?
            .ok_or_else(|| Problem::invalid("upload"))?;
        let filename = field.file_name().map(str::to_owned);
        let mut size = 0usize;
        let mut chunks = 0usize;
        let mut sum = 0usize;
        while let Some(bytes) = field.chunk().await.map_err(upload_error)? {
            size += bytes.len();
            chunks += 1;
            sum += bytes.iter().map(|value| *value as usize).sum::<usize>();
        }
        drop(field);
        assert!(reader.next_field().await?.is_none());
        Ok(json!({"size":size,"chunks":chunks,"sum":sum,"filename":filename}))
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: Value,
    ) -> Result<EndpointOutput<Value>, Infallible> {
        Ok(EndpointResponse::Json(input).into())
    }
    fn body_schema(&self) -> Value {
        json!({"type":"object","properties":{"metadata":{"type":"string"},"upload":{"type":"string","format":"binary"}}})
    }
}

fn upload_error(error: multer::Error) -> Problem {
    match error {
        multer::Error::FieldSizeExceeded { .. } | multer::Error::StreamSizeExceeded { .. } => {
            Problem::new(StatusCode::PAYLOAD_TOO_LARGE)
        }
        _ => Problem::invalid("multipart"),
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
async fn actual_urlencoded_and_multipart_forms_share_one_buffer_with_named_files() {
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/form", "form");
    contract.body = BodyMode::Form(MultipartLimits {
        total_bytes: 4096,
        section_bytes: 1024,
        sections: 8,
    });
    let reg = EndpointRegistration::prepare(&mut runtime, contract, Form).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{url}/form"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("aliased-count=3&reserved-name=a&reserved-name=b")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["count"], 3);
    assert_eq!(body["metadata"], json!(["a", "b"]));
    assert_eq!(body["files"], json!([]));
    let form = reqwest::multipart::Form::new()
        .text("reserved-name", "meta")
        .part(
            "upload",
            reqwest::multipart::Part::bytes(vec![0, 1, 255]).file_name("one.bin"),
        )
        .part(
            "upload",
            reqwest::multipart::Part::bytes(vec![2, 3]).file_name("two.bin"),
        );
    let response = client
        .post(format!("{url}/form"))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["count"], 9);
    assert_eq!(body["files"][0]["bytes"], json!([0, 1, 255]));
    assert_eq!(body["files"][1]["bytes"], json!([2, 3]));
    let oversized = reqwest::multipart::Form::new().text("metadata", "x".repeat(1025));
    assert_eq!(
        client
            .post(format!("{url}/form"))
            .multipart(oversized)
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert_eq!(
        api.specification().pointer(api.specification()["paths"]["/form"]["post"]["requestBody"]["content"]["multipart/form-data"]["schema"]["$ref"].as_str().unwrap().trim_start_matches('#')).unwrap()["properties"]["upload"]["items"]["format"],
        "binary"
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn streamed_multipart_advances_before_sender_releases_remaining_bytes() {
    let first_consumed = Arc::new(Notify::new());
    let first_count = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/stream", "stream");
    contract.body = BodyMode::Multipart(MultipartLimits {
        total_bytes: 2_000_000,
        section_bytes: 1_100_000,
        sections: 3,
    });
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        contract,
        StreamUpload {
            first_consumed: Arc::clone(&first_consumed),
            first_count: Arc::clone(&first_count),
        },
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    // The sender cannot send the file until server middleware consumes the first
    // field. A buffering implementation deadlocks and fails this bounded timeout.
    let body = stream::unfold((0usize, first_consumed), |(state, consumed)| async move {
        match state {
            0=>Some((Ok::<_,std::io::Error>(b"--native-boundary\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\nfirst\r\n--native-boundary\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"data.bin\"\r\n\r\n".to_vec()),(1,consumed))),
            1=>{consumed.notified().await;Some((Ok(vec![7u8;16_384]),(2,consumed)))},
            2..=64=>{tokio::task::yield_now().await;Some((Ok(vec![7u8;16_384]),(state+1,consumed)))},
            65=>Some((Ok(b"\r\n--native-boundary--\r\n".to_vec()),(66,consumed))),
            _=>None,
        }
    });
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reqwest::Client::new()
            .post(format!("{url}/stream"))
            .header(
                "content-type",
                "multipart/form-data; boundary=native-boundary",
            )
            .body(reqwest::Body::wrap_stream(body))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["size"], 64 * 16_384);
    assert_eq!(body["sum"], 64 * 16_384 * 7);
    assert!(body["chunks"].as_u64().unwrap() > 1);
    assert_eq!(first_count.load(Ordering::SeqCst), 1);
    let client = reqwest::Client::new();
    for (media, status) in [
        ("application/json", 415),
        ("multipart/form-data", 400),
        ("multipart/form-data; boundary=", 400),
    ] {
        assert_eq!(
            client
                .post(format!("{url}/stream"))
                .header("content-type", media)
                .body("bad")
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn streaming_section_total_and_reader_guards_are_real() {
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Get, "/bad", "bad");
    contract.body = BodyMode::Multipart(MultipartLimits::default());
    assert!(
        EndpointRegistration::prepare(
            &mut runtime,
            contract,
            StreamUpload {
                first_consumed: Arc::new(Notify::new()),
                first_count: Arc::new(AtomicUsize::new(0))
            }
        )
        .is_err()
    );
    let mut contract = EndpointContract::new(HttpMethod::Post, "/bounded", "bounded");
    contract.body = BodyMode::Multipart(MultipartLimits {
        total_bytes: 512,
        section_bytes: 100,
        sections: 2,
    });
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        contract,
        StreamUpload {
            first_consumed: Arc::new(Notify::new()),
            first_count: Arc::new(AtomicUsize::new(0)),
        },
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    for size in [101, 1024] {
        let body = format!(
            "--b\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\nfirst\r\n--b\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"file\"\r\n\r\n{}\r\n--b--\r\n",
            "x".repeat(size)
        );
        assert_eq!(
            reqwest::Client::new()
                .post(format!("{url}/bounded"))
                .header("content-type", "multipart/form-data; boundary=b")
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            413
        );
    }
    task.abort();
    let _ = task.await;
}
