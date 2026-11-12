use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use ipma_public_data_proto::v1::{
    public_data_service_client::PublicDataServiceClient,
    public_data_service_server::PublicDataServiceServer, *,
};
use ipma_public_data_service::{Proxy, ProxyOptions};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;

#[derive(Clone)]
struct Upstream {
    count: Arc<AtomicUsize>,
    status: StatusCode,
    body: Option<&'static str>,
    delay: Duration,
}
struct Harness {
    client: PublicDataServiceClient<tonic::transport::Channel>,
    count: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn handler(State(state): State<Upstream>, Path(path): Path<String>) -> impl IntoResponse {
    state.count.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(state.delay).await;
    let body = state.body.unwrap_or_else(|| match path.as_str() {
        "forecast/aggregate/1110600.json" => include_str!("fixtures/forecast.json"),
        "forecast/locations.json" => include_str!("fixtures/locations.json"),
        "districts.json" => include_str!("fixtures/districts.json"),
        "warnings/warnings_www.json" => include_str!("fixtures/warnings.json"),
        _ => panic!("unexpected upstream path: {path}"),
    });
    (state.status, body)
}
async fn harness(
    status: StatusCode,
    body: Option<&'static str>,
    delay: Duration,
    limit: usize,
    timeout: Duration,
) -> Harness {
    configured_harness(
        status,
        body,
        delay,
        ProxyOptions {
            max_body_bytes: limit,
            upstream_timeout: timeout,
            max_concurrent_fetches: 1,
            ..Default::default()
        },
    )
    .await
}

async fn configured_harness(
    status: StatusCode,
    body: Option<&'static str>,
    delay: Duration,
    mut options: ProxyOptions,
) -> Harness {
    let count = Arc::new(AtomicUsize::new(0));
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = upstream.local_addr().unwrap();
    let app = Router::new()
        .route("/public-data/{*path}", get(handler))
        .with_state(Upstream {
            count: count.clone(),
            status,
            body,
            delay,
        });
    let http = tokio::spawn(async move {
        axum::serve(upstream, app).await.unwrap();
    });
    options.upstream_base_url = format!("http://{addr}/public-data/").parse().unwrap();
    let proxy = Proxy::new(options).unwrap();
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let grpc = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PublicDataServiceServer::new(proxy))
            .serve_with_incoming(TcpListenerStream::new(grpc_listener))
            .await
            .unwrap();
    });
    let client = PublicDataServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    Harness {
        client,
        count,
        tasks: vec![http, grpc],
    }
}
async fn normal() -> Harness {
    harness(
        StatusCode::OK,
        None,
        Duration::ZERO,
        1024 * 1024,
        Duration::from_secs(2),
    )
    .await
}
#[tokio::test]
async fn all_feeds_delegate_and_proxy_never_caches() {
    let mut h = normal().await;
    assert_eq!(
        h.client
            .ping(PingRequest {})
            .await
            .unwrap()
            .into_inner()
            .message,
        "pong"
    );
    assert_eq!(h.count.load(Ordering::SeqCst), 0);
    for _ in 0..2 {
        let r = h
            .client
            .get_forecast(GetForecastRequest {
                location_id: 1110600,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(r.forecasts.iter().any(|f| f.period_hours == Some(1)));
        assert!(r.forecasts.iter().any(|f| f.period_hours == Some(3)));
        assert!(r.forecasts.iter().any(|f| f.period_hours == Some(24)));
        let m = r.metadata.unwrap();
        assert!(
            m.source_url
                .ends_with("/public-data/forecast/aggregate/1110600.json")
        );
        assert!(m.expires_at.is_none());
        assert!(m.upstream_updated_at.is_some());
    }
    assert_eq!(
        h.client
            .get_locations(GetLocationsRequest {})
            .await
            .unwrap()
            .into_inner()
            .locations
            .len(),
        2
    );
    assert_eq!(
        h.client
            .get_districts(GetDistrictsRequest {})
            .await
            .unwrap()
            .into_inner()
            .districts
            .len(),
        2
    );
    let warnings = h
        .client
        .get_warnings(GetWarningsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(warnings.metadata.unwrap().upstream_updated_at.is_some());
    assert!(!warnings.warnings.is_empty());
    assert_eq!(h.count.load(Ordering::SeqCst), 5);
}
#[tokio::test]
async fn preserves_zero_negatives_missing_values_and_unknown_fields() {
    let body = r#"[{"globalIdLocal":1110600,"dataPrev":"2026-11-12T10:00:00","idPeriodo":7,"tMed":"-2.5","hR":0,"tMax":null,"tMin":"-99.0","utci":"","ffVento":"0","idTipoTempo":777,"future":{"x":true}}]"#;
    let mut h = harness(
        StatusCode::OK,
        Some(body),
        Duration::ZERO,
        10000,
        Duration::from_secs(2),
    )
    .await;
    let r = h
        .client
        .get_forecast(GetForecastRequest {
            location_id: 1110600,
        })
        .await
        .unwrap()
        .into_inner();
    let f = &r.forecasts[0];
    assert_eq!(f.temperature_celsius, Some(-2.5));
    assert_eq!(f.relative_humidity_percent, Some(0.0));
    assert_eq!(f.wind_speed_kilometers_per_hour, Some(0.0));
    assert!(f.minimum_temperature_celsius.is_none());
    assert!(f.maximum_temperature_celsius.is_none());
    assert!(f.utci_celsius.is_none());
    assert_eq!(f.period_hours, Some(7));
    assert_eq!(f.weather_type_code, Some(777));
    let original: serde_json::Value = serde_json::from_str(&f.source_json).unwrap();
    assert_eq!(original["future"]["x"], true);
    assert_eq!(f.forecast_at.unwrap().seconds, 1794477600);
}
#[tokio::test]
async fn validates_request_before_fetch_and_rejects_schema_drift() {
    let mut h = normal().await;
    assert_eq!(
        h.client
            .get_forecast(GetForecastRequest { location_id: 0 })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(h.count.load(Ordering::SeqCst), 0);
    for body in [
        "{}",
        "[null]",
        r#"[{"globalIdLocal":1,"dataPrev":"2026-11-12T10:00:00"}]"#,
        r#"[{"globalIdLocal":1110600,"dataPrev":"bad"}]"#,
        r#"[{"globalIdLocal":1110600,"dataPrev":"2026-11-12T10:00:00","tMed":"NaN"}]"#,
    ] {
        let mut h = harness(
            StatusCode::OK,
            Some(body),
            Duration::ZERO,
            10000,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            h.client
                .get_forecast(GetForecastRequest {
                    location_id: 1110600
                })
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
    }
}
#[tokio::test]
async fn maps_http_errors_and_bounds_body_and_timeout() {
    for (http, code) in [
        (StatusCode::NOT_FOUND, Code::NotFound),
        (StatusCode::TOO_MANY_REQUESTS, Code::Unavailable),
        (StatusCode::FOUND, Code::Unavailable),
    ] {
        let mut h = harness(
            http,
            Some("[]"),
            Duration::ZERO,
            10000,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            h.client
                .get_forecast(GetForecastRequest {
                    location_id: 1110600
                })
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    let mut h = harness(
        StatusCode::OK,
        Some("[12345]"),
        Duration::ZERO,
        4,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        h.client
            .get_forecast(GetForecastRequest {
                location_id: 1110600
            })
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    let mut h = harness(
        StatusCode::OK,
        Some("[]"),
        Duration::from_millis(100),
        1000,
        Duration::from_millis(10),
    )
    .await;
    assert_eq!(
        h.client
            .get_forecast(GetForecastRequest {
                location_id: 1110600
            })
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
}
#[test]
fn validates_base_url_and_resource_limits() {
    for url in [
        "http://example.org/",
        "https://a:b@example.org/",
        "https://example.org/?x=1",
        "https://example.org/no-slash",
    ] {
        assert!(
            Proxy::new(ProxyOptions {
                upstream_base_url: url.parse().unwrap(),
                ..Default::default()
            })
            .is_err()
        );
    }
    assert!(
        Proxy::new(ProxyOptions {
            max_concurrent_fetches: 0,
            ..Default::default()
        })
        .is_err()
    );
}
