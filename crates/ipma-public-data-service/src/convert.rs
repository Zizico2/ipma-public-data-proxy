use crate::proxy::{Error, Key};
use chrono::{DateTime, NaiveDateTime};
use ipma_public_data_client::types as up;
use ipma_public_data_proto::v1 as pb;
use prost::Message;

pub(crate) enum Payload {
    Forecast(pb::GetForecastResponse),
    Locations(pb::GetLocationsResponse),
    Districts(pb::GetDistrictsResponse),
    Warnings(pb::GetWarningsResponse),
}
impl Payload {
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Forecast(v) => v.encoded_len(),
            Self::Locations(v) => v.encoded_len(),
            Self::Districts(v) => v.encoded_len(),
            Self::Warnings(v) => v.encoded_len(),
        }
    }
}
type Result<T, E = Error> = std::result::Result<T, E>;
fn invalid(field: &str) -> Error {
    Error::Invalid(format!("invalid or missing {field}"))
}
fn parse<'a, T: serde::Deserialize<'a>>(body: &'a [u8]) -> Result<T> {
    serde_json::from_slice(body).map_err(|e| Error::Invalid(e.to_string()))
}
trait SourceNumber {
    fn source_number(&self) -> Result<Option<f64>>;
}
fn numeric_string(s: &str) -> Result<Option<f64>> {
    if s.trim().is_empty() {
        return Ok(None);
    }
    s.trim()
        .parse()
        .map(Some)
        .map_err(|_| invalid("numeric string"))
}
impl SourceNumber for up::NumericValue {
    fn source_number(&self) -> Result<Option<f64>> {
        match self {
            Self::Variant0(n) => Ok(*n),
            Self::Variant1(s) => numeric_string(s),
        }
    }
}
impl SourceNumber for up::CodeValue {
    fn source_number(&self) -> Result<Option<f64>> {
        match self {
            Self::Variant0(n) => Ok(n.map(|n| n as f64)),
            Self::Variant1(s) => numeric_string(s),
        }
    }
}
fn number<T: SourceNumber>(value: &Option<T>) -> Result<Option<f64>> {
    let n = value
        .as_ref()
        .map(SourceNumber::source_number)
        .transpose()?
        .flatten();
    if n.is_some_and(|n| !n.is_finite()) {
        return Err(invalid("non-finite number"));
    }
    Ok(n.filter(|n| *n != -99.0))
}
fn integer(value: &Option<up::CodeValue>) -> Result<Option<i32>> {
    number(value)?
        .map(|n| {
            if n.fract() == 0.0 && n >= i32::MIN as f64 && n <= i32::MAX as f64 {
                Ok(n as i32)
            } else {
                Err(invalid("integer"))
            }
        })
        .transpose()
}
fn optional_id(value: &Option<up::CodeValue>) -> Result<Option<u32>> {
    number(value)?
        .map(|n| {
            if n.fract() == 0.0 && n >= 0.0 && n <= u32::MAX as f64 {
                Ok(n as u32)
            } else {
                Err(invalid("identifier"))
            }
        })
        .transpose()
}
fn id(n: std::num::NonZeroU64) -> Result<u32> {
    u32::try_from(n.get()).map_err(|_| invalid("identifier"))
}
fn text(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}
fn required_text(value: &str) -> Result<String> {
    if value.is_empty() {
        Err(invalid("text"))
    } else {
        Ok(value.to_owned())
    }
}
fn timestamp(value: Option<&str>) -> Result<Option<prost_types::Timestamp>> {
    let Some(s) = value.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let dt = DateTime::parse_from_rfc3339(s)
        .map(|t| t.to_utc())
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").map(|t| t.and_utc()))
        .map_err(|_| invalid("timestamp"))?;
    Ok(Some(prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }))
}
fn required_timestamp(value: Option<&str>) -> Result<Option<prost_types::Timestamp>> {
    timestamp(value)?
        .map(Some)
        .ok_or_else(|| invalid("timestamp"))
}
fn latest(metadata: &mut pb::SourceMetadata, t: Option<prost_types::Timestamp>) {
    if let Some(t) = t
        && metadata
            .upstream_updated_at
            .is_none_or(|old| (old.seconds, old.nanos) < (t.seconds, t.nanos))
    {
        metadata.upstream_updated_at = Some(t)
    }
}
#[derive(serde::Deserialize)]
struct RawEnvelope<'a> {
    #[serde(borrow)]
    data: Vec<&'a serde_json::value::RawValue>,
}

pub(crate) fn decode(
    key: Key,
    body: &[u8],
    mut metadata: pb::SourceMetadata,
) -> Result<Payload, Error> {
    match key {
        Key::Forecast(requested) => {
            let rows: Vec<&serde_json::value::RawValue> = parse(body)?;
            let forecasts = rows
                .into_iter()
                .map(|raw| {
                    let v: up::Forecast = parse(raw.get().as_bytes())?;
                    let location_id = id(v.global_id_local)?;
                    if location_id != requested {
                        return Err(invalid("forecast location mismatch"));
                    }
                    let updated = timestamp(v.data_update.as_deref())?;
                    latest(&mut metadata, updated);
                    Ok(pb::Forecast {
                        location_id,
                        forecast_at: required_timestamp(Some(&v.data_prev))?,
                        period_hours: integer(&v.id_periodo)?,
                        temperature_celsius: number(&v.t_med)?,
                        minimum_temperature_celsius: number(&v.t_min)?,
                        maximum_temperature_celsius: number(&v.t_max)?,
                        relative_humidity_percent: number(&v.h_r)?,
                        wind_speed_kilometers_per_hour: number(&v.ff_vento)?,
                        wind_direction: text(&v.dd_vento),
                        weather_type_code: integer(&v.id_tipo_tempo)?,
                        precipitation_probability_percent: number(&v.probabilidade_precipita)?,
                        precipitation_intensity_code: integer(&v.id_intensidade_precipita)?,
                        uv_index: number(&v.i_uv)?,
                        upstream_updated_at: updated,
                        utci_celsius: number(&v.utci)?,
                        sea_temperature_celsius: number(&v.temp_agua_mar)?,
                        total_sea_height_meters: number(&v.mar_total)?,
                        swell_height_meters: number(&v.ondulacao)?,
                        swell_period_seconds: number(&v.period_ondulacao)?,
                        peak_period_seconds: number(&v.periodo_pico)?,
                        wave_direction: text(&v.dir_ondulacao),
                        wind_speed_class: integer(&v.id_ffx_vento)?,
                        source_json: raw.get().to_owned(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Forecast(pb::GetForecastResponse {
                metadata: Some(metadata),
                forecasts,
            }))
        }
        Key::Locations => {
            let rows: Vec<&serde_json::value::RawValue> = parse(body)?;
            let locations = rows
                .into_iter()
                .map(|raw| {
                    let v: up::Location = parse(raw.get().as_bytes())?;
                    Ok(pb::Location {
                        location_id: id(v.global_id_local)?,
                        name: required_text(&v.local)?,
                        latitude: number(&v.latitude)?,
                        longitude: number(&v.longitude)?,
                        region_id: optional_id(&v.id_regiao)?,
                        district_id: optional_id(&v.id_distrito)?,
                        municipality_local_id: optional_id(&v.id_concelho)?,
                        warning_area_id: text(&v.id_area_aviso),
                        source_json: raw.get().to_owned(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Locations(pb::GetLocationsResponse {
                metadata: Some(metadata),
                locations,
            }))
        }
        Key::Districts => {
            let rows: Vec<&serde_json::value::RawValue> = parse(body)?;
            let districts = rows
                .into_iter()
                .map(|raw| {
                    let v: up::District = parse(raw.get().as_bytes())?;
                    Ok(pb::District {
                        district_id: id(v.id_distrito)?,
                        region_id: optional_id(&v.id_regiao)?,
                        name: required_text(&v.nome)?,
                        source_json: raw.get().to_owned(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Districts(pb::GetDistrictsResponse {
                metadata: Some(metadata),
                districts,
            }))
        }
        Key::Warnings => {
            let root: up::WarningsResponse = parse(body)?;
            latest(&mut metadata, timestamp(root.data_atualiza.as_deref())?);
            let source: RawEnvelope<'_> = parse(body)?;
            let warnings = root
                .data
                .into_iter()
                .zip(source.data)
                .map(|(v, raw)| {
                    Ok(pb::Warning {
                        area_id: required_text(&v.id_area_aviso)?,
                        type_name: required_text(&v.awareness_type_name)?,
                        level: required_text(&v.awareness_level_id)?,
                        text: text(&v.text),
                        starts_at: required_timestamp(Some(&v.start_time))?,
                        ends_at: required_timestamp(Some(&v.end_time))?,
                        reported_at: timestamp(v.report_date.as_deref())?,
                        source_json: raw.get().to_owned(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Warnings(pb::GetWarningsResponse {
                metadata: Some(metadata),
                warnings,
            }))
        }
    }
}
