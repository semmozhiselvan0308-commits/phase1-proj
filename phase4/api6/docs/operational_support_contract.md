# API6 Operational Support Contract

API6 is the sanctioned evidence source for the Claude Network Operations
Assistant when it needs to answer either `Can I trust this data right now?` or
`Where is this grid?`. The assistant must call these endpoints rather than
narrate pipeline or geography from memory.

## `GET /pipeline/status`

Reads the machine-readable status record written by DE7's `quality_check` and
`notify` tasks. API6 does not recompute pipeline health from raw data.

Response fields:

- `run_id`: DE7 run identifier.
- `run_timestamp`: time the status record was written.
- `tasks`: task name to final task state.
- `rows_in`, `rows_rejected`, `nulls_handled`, `rows_published`: DE7 metrics.
- `AS_OF`: the analytics timestamp used by API1.
- `analytics_age_seconds`: wall-clock age of `AS_OF`, when present.
- `analytics_freshness`: `fresh`, `stale`, or `unknown`.
- `healthy`: one boolean for caller branching.
- `reasons`: populated when `healthy` is false.

Health is true only when DE7 reports `pipeline_result: success`, every task
in the status record is `success`, and `AS_OF` is present. Historical data can
be marked `stale` while still being a successful, internally consistent run;
freshness is reported separately from the pipeline result.

A missing or malformed DE7 record returns HTTP `503`. A valid failed DE7 run
returns HTTP `200` with `healthy: false` and a populated `reasons` list.

## `GET /network/grid/{grid_id}/location`

Reads `grid_id` and `geometry_reference` from `dim_grid`, then resolves the
referenced GeoJSON cell to return its centroid. The response contains:

- `grid_id`
- `centroid_latitude`
- `centroid_longitude`
- `polygon_reference`

The full Polygon or MultiPolygon geometry is deliberately not returned. A
missing grid returns HTTP `404`; unavailable or invalid reference data returns
HTTP `503`.

## API1 agreement

The `AS_OF` value returned by `/pipeline/status` is the same analytics end
 timestamp used by API1's `/network/summary` response. Clients can compare
that value before trusting a summary.
