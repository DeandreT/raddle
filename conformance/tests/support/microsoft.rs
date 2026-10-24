//! Trusted endpoint configuration shared by the official emulator test binaries.

use std::error::Error;
use std::net::SocketAddr;

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub fn endpoint() -> TestResult<SocketAddr> {
    if std::env::var("RADDLE_MICROSOFT_TEST_CONFIRM").as_deref() != Ok("isolated") {
        return Err(
            "set RADDLE_MICROSOFT_TEST_CONFIRM=isolated only for the dedicated fixture".into(),
        );
    }
    let endpoint: SocketAddr = std::env::var("RADDLE_MICROSOFT_TEST_ENDPOINT")?.parse()?;
    if !endpoint.ip().is_loopback() {
        return Err("the official emulator fixture must use numeric loopback".into());
    }
    Ok(endpoint)
}

pub fn connection_string(endpoint: SocketAddr) -> TestResult<String> {
    let mut service = url::Url::parse("sb://localhost/")?;
    service
        .set_ip_host(endpoint.ip())
        .map_err(|()| "invalid loopback host")?;
    // Microsoft requires logical link-address and connection OPEN hosts to match.
    // These are public emulator-only credentials, not real Azure credentials.
    Ok(format!(
        "Endpoint={service};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
    ))
}
