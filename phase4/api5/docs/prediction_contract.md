# API5 Prediction Contract

## Endpoint

`POST /network/predict-risk`

The endpoint accepts the feature record used to predict network risk. All
request fields are required, and unknown fields are rejected.

## Request body

```json
{
  "grid_id": 4821,
  "feature_timestamp": "2013-11-07T19:00:00Z",
  "avg_activity": 42.5,
  "activity_growth": 0.12,
  "active_hours": 18.0,
  "peak_ratio": 1.4,
  "variability": 0.25,
  "internet_share": 0.85
}
```

| Field | Type | Constraints |
| --- | --- | --- |
| `grid_id` | integer | Greater than 0 |
| `feature_timestamp` | ISO datetime | Required |
| `avg_activity` | number | Greater than or equal to 0 |
| `activity_growth` | number | Required; may be negative |
| `active_hours` | number | Between 0 and 24 |
| `peak_ratio` | number | Greater than or equal to 0 |
| `variability` | number | Greater than or equal to 0 |
| `internet_share` | number | Between 0 and 1 |

## Response body

```json
{
  "risk_score": 0.0,
  "risk_level": "UNKNOWN",
  "model_version": "stub-v1",
  "explanation_note": "Stub implementation: no trained ML model is wired in yet."
}
```

The response keys and meanings are stable. The current implementation is a
stub and always returns the values above. ML5 must preserve this response
shape and the request validation contract when replacing the stub with the
trained model. `risk_score` must remain normalized between 0 and 1.

## Validation errors

Invalid request bodies return HTTP `422` with FastAPI's standard structured
validation response. The `detail` array identifies the invalid field in
`loc` and includes a readable `msg`, for example:

```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "internet_share"],
      "msg": "Field required"
    }
  ]
}
```
