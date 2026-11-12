use crate::proxy::{Error, Key};
use chrono::{DateTime, NaiveDateTime};
use ipma_public_data_proto::v1 as pb;
use prost::Message;
use serde_json::Value;

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
fn invalid(field: &str) -> Error {
    Error::Invalid(format!("invalid or missing {field}"))
}
fn rows(v: &Value) -> Result<&Vec<Value>, Error> {
    v.as_array().ok_or_else(|| invalid("array"))
}
fn object(v: &Value) -> Result<(), Error> {
    v.as_object().map(|_| ()).ok_or_else(|| invalid("record"))
}
fn text(v: &Value, field: &str) -> Result<String, Error> {
    match &v[field] {
        Value::Null => Ok(String::new()),
        Value::String(s) => Ok(s.clone()),
        _ => Err(invalid(field)),
    }
}
fn required_text(v: &Value, field: &str) -> Result<String, Error> {
    let s = text(v, field)?;
    if s.is_empty() {
        return Err(invalid(field));
    }
    Ok(s)
}
fn number(v: &Value, field: &str) -> Result<Option<f64>, Error> {
    let n = match &v[field] {
        Value::Null => return Ok(None),
        Value::String(s) if s.trim().is_empty() => return Ok(None),
        Value::String(s) => s.trim().parse().map_err(|_| invalid(field))?,
        Value::Number(n) => n.as_f64().ok_or_else(|| invalid(field))?,
        _ => return Err(invalid(field)),
    };
    if !n.is_finite() {
        return Err(invalid(field));
    }
    Ok((n != -99.0).then_some(n))
}
fn integer(v: &Value, field: &str) -> Result<Option<i32>, Error> {
    number(v, field)?
        .map(|n| {
            if n.fract() != 0.0 || n < i32::MIN as f64 || n > i32::MAX as f64 {
                Err(invalid(field))
            } else {
                Ok(n as i32)
            }
        })
        .transpose()
}
fn optional_id(v: &Value, field: &str) -> Result<Option<u32>, Error> {
    number(v, field)?
        .map(|n| {
            if n.fract() != 0.0 || n < 0.0 || n > u32::MAX as f64 {
                Err(invalid(field))
            } else {
                Ok(n as u32)
            }
        })
        .transpose()
}
fn id(v: &Value, field: &str) -> Result<u32, Error> {
    optional_id(v, field)?
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid(field))
}
fn timestamp(v: &Value, field: &str) -> Result<Option<prost_types::Timestamp>, Error> {
    let s = text(v, field)?;
    if s.is_empty() {
        return Ok(None);
    }
    let dt = DateTime::parse_from_rfc3339(&s)
        .map(|t| t.to_utc())
        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f").map(|t| t.and_utc()))
        .map_err(|_| invalid(field))?;
    Ok(Some(prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }))
}
fn required_timestamp(v: &Value, field: &str) -> Result<Option<prost_types::Timestamp>, Error> {
    timestamp(v, field)?.map(Some).ok_or_else(|| invalid(field))
}
fn latest(metadata: &mut pb::SourceMetadata, t: Option<prost_types::Timestamp>) {
    if let Some(t) = t
        && metadata
            .upstream_updated_at
            .is_none_or(|old| (old.seconds, old.nanos) < (t.seconds, t.nanos))
    {
        metadata.upstream_updated_at = Some(t);
    }
}
pub(crate) fn decode(
    key: Key,
    body: &[u8],
    mut metadata: pb::SourceMetadata,
) -> Result<Payload, Error> {
    let root: Value = serde_json::from_slice(body).map_err(|e| Error::Invalid(e.to_string()))?;
    match key {
        Key::Forecast(requested) => {
            let forecasts = rows(&root)?
                .iter()
                .map(|v| {
                    object(v)?;
                    let location_id = id(v, "globalIdLocal")?;
                    if location_id != requested {
                        return Err(invalid("forecast location mismatch"));
                    }
                    let updated = timestamp(v, "dataUpdate")?;
                    latest(&mut metadata, updated);
                    Ok(pb::Forecast {
                        location_id,
                        forecast_at: required_timestamp(v, "dataPrev")?,
                        period_hours: integer(v, "idPeriodo")?,
                        temperature_celsius: number(v, "tMed")?,
                        minimum_temperature_celsius: number(v, "tMin")?,
                        maximum_temperature_celsius: number(v, "tMax")?,
                        relative_humidity_percent: number(v, "hR")?,
                        wind_speed_kilometers_per_hour: number(v, "ffVento")?,
                        wind_direction: text(v, "ddVento")?,
                        weather_type_code: integer(v, "idTipoTempo")?,
                        precipitation_probability_percent: number(v, "probabilidadePrecipita")?,
                        precipitation_intensity_code: integer(v, "idIntensidadePrecipita")?,
                        uv_index: number(v, "iUv")?,
                        upstream_updated_at: updated,
                        utci_celsius: number(v, "utci")?,
                        sea_temperature_celsius: number(v, "tempAguaMar")?,
                        total_sea_height_meters: number(v, "marTotal")?,
                        swell_height_meters: number(v, "ondulacao")?,
                        swell_period_seconds: number(v, "periodOndulacao")?,
                        peak_period_seconds: number(v, "periodoPico")?,
                        wave_direction: text(v, "dirOndulacao")?,
                        wind_speed_class: integer(v, "idFfxVento")?,
                        source_json: v.to_string(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Forecast(pb::GetForecastResponse {
                metadata: Some(metadata),
                forecasts,
            }))
        }
        Key::Locations => {
            let locations = rows(&root)?
                .iter()
                .map(|v| {
                    object(v)?;
                    Ok(pb::Location {
                        location_id: id(v, "globalIdLocal")?,
                        name: required_text(v, "local")?,
                        latitude: number(v, "latitude")?,
                        longitude: number(v, "longitude")?,
                        region_id: optional_id(v, "idRegiao")?,
                        district_id: optional_id(v, "idDistrito")?,
                        municipality_local_id: optional_id(v, "idConcelho")?,
                        warning_area_id: text(v, "idAreaAviso")?,
                        source_json: v.to_string(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Locations(pb::GetLocationsResponse {
                metadata: Some(metadata),
                locations,
            }))
        }
        Key::Districts => {
            let districts = rows(&root)?
                .iter()
                .map(|v| {
                    object(v)?;
                    Ok(pb::District {
                        district_id: id(v, "idDistrito")?,
                        region_id: optional_id(v, "idRegiao")?,
                        name: required_text(v, "nome")?,
                        source_json: v.to_string(),
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Payload::Districts(pb::GetDistrictsResponse {
                metadata: Some(metadata),
                districts,
            }))
        }
        Key::Warnings => {
            object(&root)?;
            latest(&mut metadata, timestamp(&root, "dataAtualiza")?);
            let warnings = rows(&root["data"])?
                .iter()
                .map(|v| {
                    object(v)?;
                    Ok(pb::Warning {
                        area_id: required_text(v, "idAreaAviso")?,
                        type_name: required_text(v, "awarenessTypeName")?,
                        level: required_text(v, "awarenessLevelID")?,
                        text: text(v, "text")?,
                        starts_at: required_timestamp(v, "startTime")?,
                        ends_at: required_timestamp(v, "endTime")?,
                        reported_at: timestamp(v, "reportDate")?,
                        source_json: v.to_string(),
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
