# ML1 Network Activity Risk Problem Statement

## Decision

The primary operational ML problem is **next-hour high-activity investigation
risk**.

The model predicts whether a grid's next hourly activity interval is likely to
be unusually high relative to that grid's own historical operating pattern.
A positive prediction means **a human operator should investigate the grid**.
It does not mean that the network is congested or that a fault has occurred.

## Prediction unit

One observation is one **grid plus one hourly time window**:

- Grid: one Milan grid identified by `grid_id`.
- Feature time: the trailing 24 hourly observations ending at hour `t`.
- Prediction time: the immediately following hourly observation at `t+1`.

The chronological boundary is explicit:

```text
features: [t-23, ..., t-2, t-1, t]
label:                activity at t+1
```

A feature row may only use values available at the end of hour `t`.

## Label definition

The binary training label is:

```text
y(t+1) = 1 when total_activity(grid, t+1)
        > the grid-specific 90th percentile of total_activity
          computed from the training-period history only

        = 0 otherwise
```

The 90th-percentile threshold is a **training proxy** for an unusually high
future activity interval. It is not a capacity threshold and it is not a
measurement of congestion. The threshold must be fitted on the training
period only and then held fixed for validation and test periods.

The label describes the future interval `t+1`; it is never computed from the
same trailing window used to make the features.

## Candidate alternatives considered

| Candidate | Decision | Reason |
| --- | --- | --- |
| High-activity risk | **Selected** | Directly supports an investigation queue and aligns with available activity data. |
| Anomalous activity | Deferred | Useful, but requires a stronger definition of baseline deviation and may overlap with the selected proxy. |
| Activity drop | Deferred | Operationally meaningful, but less aligned with the current risk endpoint and alert workflow. |

## Feature evidence

The first feature set uses only trailing-window information available at `t`:

- `avg_activity`: mean total activity over the trailing window.
- `activity_growth`: recent activity change versus the trailing baseline.
- `active_hours`: number of active hours in the trailing window.
- `peak_ratio`: trailing-window peak relative to its average.
- `variability`: dispersion of trailing activity.
- `internet_share`: trailing-window internet activity share.

The model may use the temporal pattern in these features to estimate future
risk. It is not allowed to use `total_activity` at `t+1` or any later interval.

## Leakage controls

1. **Future target leakage:** `t+1` activity is used only to create the label,
   never as a feature. The feature query is cut off at `t`.
2. **Threshold leakage:** each grid's 90th-percentile threshold is fitted from
   the chronological training period only. Validation and test observations
   cannot influence it.
3. **Window leakage:** rolling features are right-aligned and use only the
   trailing 24 hours. No centered or forward-looking rolling window is allowed.
4. **Split leakage:** train, validation, and test sets are chronological, not
   randomly shuffled. A later observation must never train a prediction for
   an earlier observation.
5. **Duplicate interval leakage:** each grid-hour appears once in the feature
   table before the chronological split.

## Business action

When the model predicts a positive result, the operator should:

1. Open the grid's recent activity view.
2. Compare the signal with nearby grids and current alerts.
3. Investigate the underlying activity pattern.
4. Escalate only if independent operational evidence supports escalation.

The model output is an investigation priority, not an automated incident
conclusion.

## Non-goals and forbidden claims

This model does **not** claim:

- that the network is congested;
- that the grid has insufficient capacity;
- that throughput is degraded;
- that latency or packet loss is elevated;
- that a fault, outage, or service incident has occurred;
- that high activity is harmful by itself;
- that the prediction is causal or certain.

The available data contains activity counts and derived summaries, but no
capacity, throughput, latency, packet-loss, or radio-utilization evidence.

## What the model knows beyond a simple threshold

At prediction time, the model can combine the trailing temporal pattern,
growth, peak shape, variability, and activity mix to estimate the probability
of a **future** high-activity interval, rather than merely restating whether
the current interval already crossed a threshold.

## Review checklist for ML2 and ML3

- Preserve the `t`/`t+1` boundary when building features and labels.
- Report class balance after applying the grid-specific training thresholds.
- Compare the model with a transparent baseline.
- Use precision, recall, and a confusion matrix in addition to accuracy.
- Interpret a positive result as `investigate`, never as `congested`.
