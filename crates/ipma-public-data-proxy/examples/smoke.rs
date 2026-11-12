use ipma_public_data_proto::{
    MAX_MESSAGE_BYTES,
    v1::{self as pb, public_data_service_client::PublicDataServiceClient},
};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50053".into());
    let mut client = PublicDataServiceClient::connect(address)
        .await?
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
    anyhow::ensure!(
        client.ping(pb::PingRequest {}).await?.into_inner().message == "pong",
        "unexpected ping"
    );
    let locations = client
        .get_locations(pb::GetLocationsRequest {})
        .await?
        .into_inner();
    anyhow::ensure!(!locations.locations.is_empty(), "empty catalogue");
    let districts = client
        .get_districts(pb::GetDistrictsRequest {})
        .await?
        .into_inner();
    anyhow::ensure!(!districts.districts.is_empty(), "empty districts");
    client.get_warnings(pb::GetWarningsRequest {}).await?;
    let forecast = client
        .get_forecast(pb::GetForecastRequest {
            location_id: 1110600,
        })
        .await?
        .into_inner();
    anyhow::ensure!(
        forecast.forecasts.iter().any(|f| f.period_hours == Some(1)),
        "no hourly forecasts"
    );
    println!("PASS: all four website feeds and hourly forecast over native gRPC");
    Ok(())
}
