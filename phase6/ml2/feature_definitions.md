# ML2 Feature Definitions

ML1 fixes the prediction boundary: features for a prediction about `t+1` may
see the trailing window ending at `t`, inclusive, and may not see anything
later.

## Approved windows

- Recent window: 24 hours, `[t-23, ..., t]`.
- Baseline window: the immediately preceding 24 hours, `[t-47, ..., t-24]`.
- Feature timestamp: `t`, the final interval visible to every feature.
- First complete feature row: after 48 hourly observations are available.

## Features

| Feature | Formula | Operational meaning | Leakage rule |
| --- | --- | --- | --- |
| `avg_activity` | Mean `total_activity` over `[t-23, t]` | Recent activity level | Query ends at `t`. |
| `activity_growth` | `(recent_mean - baseline_mean) / baseline_mean` | Change against the prior 24-hour baseline | Both means use windows at or before `t`. If baseline is zero, return `1.0` when recent activity is positive, otherwise `0.0`. |
| `active_hours` | Count of recent hours where `total_activity > 0` | How consistently the grid was active | Count includes only `[t-23, t]`. |
| `peak_ratio` | `max(recent_total_activity) / avg_activity` | Peakiness relative to the recent level | Maximum and average stop at `t`; zero average returns `0.0`. |
| `variability` | Population standard deviation of recent `total_activity` | Spread of recent activity | Dispersion uses only `[t-23, t]`. |
| `internet_share` | `sum(recent_internet_activity) / sum(recent_total_activity)` | Recent activity mix | Both sums stop at `t`; zero total returns `0.0`. |

The persisted table is `network_feature_table` with `grid_id`,
`feature_timestamp`, and these six feature columns. The implementation uses
SQLite warehouse rows and explicitly left-joins missing fact rows as zero.
