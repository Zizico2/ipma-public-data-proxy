use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use bytes::BytesMut;
use futures_util::StreamExt;
use ipma_public_data_client::raw::Client as ApiClient;
use ipma_public_data_proto::{
    MAX_MESSAGE_BYTES,
    v1::{self as pb, public_data_service_server::PublicDataService},
};
use reqwest::{Client as HttpClient, Url};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use crate::convert::{self, Payload};

#[derive(Debug, Clone)]
pub struct ProxyOptions {
    pub upstream_base_url: Url,
    pub upstream_timeout: Duration,
    pub max_body_bytes: usize,
    pub max_concurrent_fetches: usize,
}

impl Default for ProxyOptions {
    fn default() -> Self {
        Self {
            upstream_base_url: Url::parse("https://api.ipma.pt/public-data/").expect("static URL"),
            upstream_timeout: Duration::from_secs(15),
            max_body_bytes: MAX_MESSAGE_BYTES,
            max_concurrent_fetches: 8,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("upstream request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream returned HTTP {0}")]
    HttpStatus(u16),
    #[error("upstream body or converted response exceeds size limit")]
    TooLarge,
    #[error("proxy fetch capacity exhausted")]
    Busy,
    #[error("invalid upstream data: {0}")]
    Invalid(String),
    #[error("conversion worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
}

impl From<ipma_public_data_client::Error<()>> for Error {
    fn from(error: ipma_public_data_client::Error<()>) -> Self {
        use ipma_public_data_client::Error as ClientError;
        match error {
            ClientError::CommunicationError(error) | ClientError::ResponseBodyError(error) => {
                Self::Http(error)
            }
            error => match error.status() {
                Some(status) => Self::HttpStatus(status.as_u16()),
                None => Self::Invalid(error.to_string()),
            },
        }
    }
}

impl Error {
    fn status(&self) -> Status {
        match self {
            Self::Http(e) if e.is_timeout() => Status::deadline_exceeded("IPMA request timed out"),
            Self::Http(_) | Self::HttpStatus(429 | 500..=599) => {
                Status::unavailable("IPMA is unavailable")
            }
            Self::HttpStatus(404) => Status::not_found("IPMA dataset was not found"),
            Self::HttpStatus(_) => Status::unavailable("IPMA rejected the upstream request"),
            Self::Busy | Self::TooLarge => Status::resource_exhausted(self.to_string()),
            Self::Invalid(_) => Status::data_loss("IPMA returned invalid or unsupported data"),
            Self::Worker(_) => Status::internal("dataset conversion failed"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Key {
    Forecast(u32),
    Locations,
    Districts,
    Warnings,
}
impl Key {
    pub(crate) fn path(self) -> String {
        match self {
            Self::Forecast(id) => format!("forecast/aggregate/{id}.json"),
            Self::Locations => "forecast/locations.json".into(),
            Self::Districts => "districts.json".into(),
            Self::Warnings => "warnings/warnings_www.json".into(),
        }
    }
}

/// Clones share the HTTP connection pool and fetch permits. Each data RPC fetches IPMA.
#[derive(Clone)]
pub struct Proxy {
    client: ApiClient,
    options: ProxyOptions,
    permits: Arc<Semaphore>,
}

impl Proxy {
    pub fn new(options: ProxyOptions) -> anyhow::Result<Self> {
        let url = &options.upstream_base_url;
        anyhow::ensure!(
            url.scheme() == "https" || (url.scheme() == "http" && is_loopback(url)),
            "upstream must use HTTPS (HTTP allowed only for loopback tests)"
        );
        anyhow::ensure!(
            url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path().ends_with('/'),
            "upstream base must be an absolute directory URL without credentials, query or fragment"
        );
        anyhow::ensure!(
            options.max_body_bytes > 0 && options.max_body_bytes <= MAX_MESSAGE_BYTES,
            "body limit must be in 1..={MAX_MESSAGE_BYTES}"
        );
        anyhow::ensure!(
            options.max_concurrent_fetches > 0 && !options.upstream_timeout.is_zero(),
            "resource limits and timeout must be positive"
        );
        anyhow::ensure!(
            options.max_concurrent_fetches <= 256
                && options.upstream_timeout <= Duration::from_secs(300),
            "fetch concurrency must not exceed 256 and timeout must not exceed 300 seconds"
        );
        let client = HttpClient::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .connect_timeout(options.upstream_timeout.min(Duration::from_secs(5)))
            .timeout(options.upstream_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(options.max_concurrent_fetches)
            .tcp_nodelay(true)
            .gzip(true)
            .build()?;
        let client = ApiClient::new_with_client(
            options.upstream_base_url.as_str().trim_end_matches('/'),
            client,
        );
        let permits = Arc::new(Semaphore::new(options.max_concurrent_fetches));
        Ok(Self {
            client,
            options,
            permits,
        })
    }

    async fn get(&self, key: Key) -> Result<Payload, Status> {
        self.fetch(key).await.map_err(|error| {
            tracing::warn!(path = %key.path(), %error, "dataset request failed");
            error.status()
        })
    }

    async fn request(
        &self,
        key: Key,
    ) -> Result<ipma_public_data_client::ResponseValue<ipma_public_data_client::ByteStream>, Error>
    {
        let response = match key {
            Key::Forecast(id) => {
                self.client
                    .get_forecast(
                        u64::from(id)
                            .try_into()
                            .map_err(|_| Error::Invalid("invalid location ID".into()))?,
                    )
                    .await
            }
            Key::Locations => self.client.get_locations().await,
            Key::Districts => self.client.get_districts().await,
            Key::Warnings => self.client.get_warnings().await,
        };
        response.map_err(Into::into)
    }

    async fn fetch(&self, key: Key) -> Result<Payload, Error> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let url = self
            .options
            .upstream_base_url
            .join(&key.path())
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let started = Instant::now();
        let response = self.request(key).await?;
        let limit = self.options.max_body_bytes;
        let length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if length.is_some_and(|n| n > limit as u64) {
            return Err(Error::TooLarge);
        }
        let mut body = BytesMut::with_capacity(length.unwrap_or(8192).min(limit as u64) as usize);
        let mut stream = response.into_inner_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > limit - body.len() {
                return Err(Error::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        let fetched = SystemTime::now();
        let metadata = pb::SourceMetadata {
            source_url: url.into(),
            fetched_at: Some(fetched.into()),
            expires_at: None,
            upstream_updated_at: None,
            attribution: "IPMA, I.P. — https://www.ipma.pt".into(),
        };
        let body = body.freeze();
        let payload = tokio::task::spawn_blocking(move || {
            // Hold the permit during CPU work, even if the requesting RPC is cancelled.
            let _permit = permit;
            let payload = convert::decode(key, &body, metadata)?;
            if payload.encoded_len() > MAX_MESSAGE_BYTES {
                return Err(Error::TooLarge);
            }
            Ok(payload)
        })
        .await??;
        tracing::info!(path = %key.path(), elapsed_ms = started.elapsed().as_millis(), response_bytes = payload.encoded_len(), "dataset fetched");
        Ok(payload)
    }
}

fn is_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}

macro_rules! respond {
    ($self:ident, $key:expr, $variant:ident) => {
        match $self.get($key).await? {
            Payload::$variant(value) => Ok(Response::new(value)),
            _ => Err(Status::internal("converted payload type mismatch")),
        }
    };
}
#[tonic::async_trait]
impl PublicDataService for Proxy {
    async fn ping(
        &self,
        _: Request<pb::PingRequest>,
    ) -> Result<Response<pb::PingResponse>, Status> {
        Ok(Response::new(pb::PingResponse {
            message: "pong".into(),
        }))
    }
    async fn get_forecast(
        &self,
        req: Request<pb::GetForecastRequest>,
    ) -> Result<Response<pb::GetForecastResponse>, Status> {
        let id = req.into_inner().location_id;
        if id == 0 {
            return Err(Status::invalid_argument("location_id must be positive"));
        }
        respond!(self, Key::Forecast(id), Forecast)
    }
    async fn get_locations(
        &self,
        _: Request<pb::GetLocationsRequest>,
    ) -> Result<Response<pb::GetLocationsResponse>, Status> {
        respond!(self, Key::Locations, Locations)
    }
    async fn get_districts(
        &self,
        _: Request<pb::GetDistrictsRequest>,
    ) -> Result<Response<pb::GetDistrictsResponse>, Status> {
        respond!(self, Key::Districts, Districts)
    }
    async fn get_warnings(
        &self,
        _: Request<pb::GetWarningsRequest>,
    ) -> Result<Response<pb::GetWarningsResponse>, Status> {
        respond!(self, Key::Warnings, Warnings)
    }
}
