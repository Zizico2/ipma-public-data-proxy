# ipma-public-data-proxy

Transparent gRPC adapter for IPMA website JSON feeds under `/public-data/`.
Every accepted data RPC performs one upstream HTTP request; no caching, retries,
redirect following or request coalescing. This is separate from `ipma-proxy` and
the documented `/open-data/` integration. Both sources are hosted by IPMA, but
`/public-data/` is the website feed, without a published API specification found.

```text
client -> ipma-public-data-gateway :50054 -> ipma-public-data-proxy :50053
                                                   -> IPMA /public-data/ HTTPS
```

The independent contract is [ipma-public-data-proto](https://github.com/Zizico2/ipma-public-data-proto),
pinned to a full Git SHA. No sibling repository, local Cargo override, protoc or
Buf installation is needed to compile Rust. Reflection and standard gRPC health
checks advertise `ipma.public_data.v1.PublicDataService`.

## Run and verify

Rust is pinned to 1.99.1. Run the proxy before starting the gateway.

```sh
rtk cargo run --locked
rtk just ping
rtk just verify
rtk cargo run --locked --example smoke -- http://127.0.0.1:50053
```

`just` recipes use RTK, matching the development workflow. The binary itself has
no RTK dependency. CLI options override environment, then optional `.env`;
existing environment values win. See `.env.example` and `--help` for all options.
Default port: **50053**. The Docker/Cloud Run setup belongs in
[ipma-infra](https://github.com/Zizico2/ipma-infra).

## API

| RPC | Website feed relative to `https://api.ipma.pt/public-data/` |
| --- | --- |
| GetForecast | `forecast/aggregate/{location_id}.json` (positive ID required) |
| GetLocations | `forecast/locations.json` |
| GetDistricts | `districts.json` |
| GetWarnings | `warnings/warnings_www.json` |

Forecasts preserve hourly, three-hourly and daily rows. Filter `period_hours == 1`
and future UTC instants for a next-ten-hours view; do not interpolate missing data.
Location coverage comes from the website catalogue, not the smaller open-data list.
The feed can change; fixture tests are offline and the smoke example checks live
responses. It does not promise a fixed horizon. Precipitation probability retains
the website's three-hour definition. See the proto README for field semantics.

## Transport and conversion

The shared reqwest client validates TLS and negotiates gzip. HTTP is permitted
only for loopback tests. The configured base must be a directory URL without
credentials, query or fragment. Clients select fixed routes, never arbitrary URLs.
Decompressed body and encoded response limits are 32 MiB maximum; concurrency is
bounded (default 8). Conversion runs on blocking workers whose permits survive
RPC cancellation. gRPC requests are limited to 64 KiB. Shutdown handles SIGTERM.

Missing, null, empty and -99 measurements become absent optional fields. Real
zeroes and negative temperatures survive. Numbers may be JSON numbers or numeric
strings. Malformed fields, invalid timestamps or mismatched location IDs return
DATA_LOSS. Source JSON preserves unknown fields and codes. No coverage or weather
categories are invented. Unknown period codes remain available.

HTTP 404 maps to NOT_FOUND; timeouts to DEADLINE_EXCEEDED; rate limits, upstream
server errors and redirects to UNAVAILABLE; resource limits to RESOURCE_EXHAUSTED.
Ping is local and never contacts IPMA. Fetch/update timestamps and attribution
are explicit; cache expiry is left unset. Source update is the newest supplied
row update, not a promise that every forecast row has the same update time.


Feed discovery: [IPMA website JavaScript](https://www.ipma.pt/opencms/bin/scripts/otempo/prev.localidade.hora.js).
Fixtures and behavior were verified on 2026-11-12. Reuse remains subject to IPMA
terms and attribution. This project is not affiliated with IPMA.
