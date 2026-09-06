# DE5 — Storage Strategy & Data Zones

## 1. Objective

The purpose of this storage strategy is to define how telecom data is stored across the landing, raw, rejected, reference, processed, analytics, and logs zones.

The strategy separates incoming data, immutable source data, transformed data, analytical outputs, reference data, and operational audit information.

The design follows lake/lakehouse principles by keeping source data auditable while using Parquet for efficient analytical processing.

---

## 2. Storage Strategy

| Zone              | Purpose                                       | Format                     | Write Mode                                                       | Partitioning                              | Retention                                                         |
| ----------------- | --------------------------------------------- | -------------------------- | ---------------------------------------------------------------- | ----------------------------------------- | ----------------------------------------------------------------- |
| `data/landing/`   | Incoming daily source files                   | CSV                        | Append / new files                                               | None                                      | Short-term; retain until ingestion and validation succeed         |
| `data/raw/`       | Immutable accepted source records             | CSV                        | Append-only                                                      | Date organization may be used when useful | Long-term for audit and reproducibility                           |
| `data/rejected/`  | Records rejected during validation            | CSV                        | Append                                                           | Date-based when volume requires it        | Medium-term for investigation and data-quality audit              |
| `data/reference/` | Static geographic reference data              | GeoJSON                    | Controlled replacement when reference data changes               | **None**                                  | Long-term; preserve versions when required                        |
| `data/processed/` | Cleaned and transformed datasets              | Parquet                    | Append for new dates; date-level overwrite for controlled reruns | **By date**                               | Medium/long-term                                                  |
| `data/analytics/` | Curated analytical outputs and warehouse data | Parquet / warehouse tables | Append/upsert or controlled overwrite depending on dataset       | Only where it provides query benefits     | According to reporting/business requirements                      |
| `logs/`           | Pipeline execution, audit, and run history    | Log/text/JSON              | Append                                                           | Organized by execution date where useful  | Short/medium-term according to operational and audit requirements |

---

## 3. Storage Zone Details

### 3.1 Landing Zone

Location:

```text
data/landing/
```

The landing zone contains incoming daily CSV files received from the source system.

Example:

```text
data/landing/
├── sms-call-internet-mi-2013-11-01.csv
├── sms-call-internet-mi-2013-11-02.csv
└── sms-call-internet-mi-2013-11-03.csv
```

Landing data is an ingestion area and does not need date partitioning because the source files already contain the date in their filenames.

New source files are added rather than modifying existing files.

Retention can be shorter than raw because landing is primarily an arrival/staging area.

---

### 3.2 Raw Zone

Location:

```text
data/raw/
```

The raw zone contains accepted source records preserved in their original form.

Raw data is **immutable** and must not be modified in place.

The raw layer is retained because it provides an auditable representation of what was accepted from the source.

If raw data were modified, it could become impossible to:

* reproduce historical pipeline runs;
* investigate data-quality problems;
* compare processed data with the original accepted source;
* determine what data was originally received;
* rerun transformations against the original input;
* establish an audit trail for changes.

Therefore the raw write policy is:

**Append-only. Existing raw data must never be overwritten or altered.**

---

### 3.3 Rejected Zone

Location:

```text
data/rejected/
```

This zone stores records that fail validation or data-quality rules.

Rejected records should be retained separately from accepted raw data so that bad records do not contaminate the trusted raw layer.

The recommended write mode is append.

If rejection volume becomes large, the zone may be organized by processing date.

---

### 3.4 Reference Zone

Location:

```text
data/reference/
```

Current reference data:

```text
data/reference/milano-grid.geojson
```

The GeoJSON is static reference data describing the geographic grid.

It is not daily event/activity data, so it must **not be date-partitioned**.

The correct structure is:

```text
data/reference/
└── milano-grid.geojson
```

Not:

```text
data/reference/
├── date=2013-11-01/
├── date=2013-11-02/
└── ...
```

The reference file is replaced only when the underlying reference dataset is intentionally updated.

Historical versions may be retained separately when reproducibility requires them.

---

## 4. Processed Zone

Location:

```text
data/processed/
```

Processed datasets should use **Parquet** because Parquet is columnar, compact, and efficient for analytical workloads and Spark processing.

The recommended structure is:

```text
data/processed/
├── activity/
│   ├── date=2013-11-01/
│   ├── date=2013-11-02/
│   └── date=2013-11-03/
│
└── hourly/
    ├── date=2013-11-01/
    ├── date=2013-11-02/
    └── date=2013-11-03/
```

Date partitioning is useful here because telecom activity is naturally associated with dates and analytical queries commonly filter by date.

For new dates, data is appended by creating a new partition.

For a controlled rerun of an existing date, only that date's partition should be replaced rather than overwriting the entire processed dataset.

---

## 5. Analytics Zone

Location:

```text
data/analytics/
```

The analytics zone contains curated datasets intended for dashboards, reporting, and analytical consumption.

Possible structure:

```text
data/analytics/
├── dashboard/
├── grid_summary/
└── warehouse/
```

Curated Parquet is appropriate for analytical datasets.

Warehouse tables may be used when SQL-based querying and reporting are required.

Partitioning should only be introduced when it provides a measurable query or storage benefit.

Small static analytical outputs should not be unnecessarily partitioned.

Write semantics depend on the dataset:

* historical facts: append or upsert;
* derived snapshots: controlled overwrite;
* rebuilt daily outputs: date-level overwrite;
* warehouse dimensions: update/upsert where appropriate.

---

## 6. Logs Zone

Location:

```text
logs/
```

Logs are part of the storage strategy and are not treated as temporary files.

The logs zone contains:

* pipeline execution history;
* validation results;
* record counts;
* errors and warnings;
* processing timestamps;
* audit information;
* run status.

Example:

```text
logs/
├── 2013-11-01/
├── 2013-11-02/
└── 2013-11-03/
```

Logs should normally be written in append mode.

Date-based organization is useful for locating and retaining operational history.

Logs may have a shorter retention period than raw data, but they should be retained long enough to support operational troubleshooting and audit requirements.

---

## 7. Append vs Overwrite Policy

Different zones use different write semantics.

| Zone      | Recommended Semantics                                                       |
| --------- | --------------------------------------------------------------------------- |
| Landing   | Append / add new source files                                               |
| Raw       | **Append-only; never overwrite existing accepted data**                     |
| Rejected  | Append                                                                      |
| Reference | Controlled replacement when reference version changes                       |
| Processed | Append new dates; overwrite only the affected date during controlled reruns |
| Analytics | Append/upsert or controlled overwrite depending on dataset                  |
| Logs      | Append                                                                      |

A global overwrite rule is deliberately avoided because each zone has a different business purpose.

---

## 8. Retention Strategy

### Raw

Raw data should have the longest retention because it supports auditability, reproducibility, and historical reprocessing.

### Landing

Landing files can have shorter retention after successful ingestion and validation, subject to organizational requirements.

### Rejected

Rejected records should be retained long enough to investigate data-quality issues and demonstrate validation behavior.

### Reference

Reference data should be retained according to its versioning and reproducibility requirements.

### Processed

Processed Parquet can be retained for the period required for analytics and reprocessing.

### Analytics

Analytics retention should follow reporting and business requirements.

### Logs

Logs should be retained long enough to support troubleshooting, operational monitoring, and audit requirements, with older logs eligible for archival or deletion according to policy.

---

## 9. Partitioning Principle

Partitioning should be used only when it provides a practical benefit.

The primary partitioning strategy is:

```text
data/processed/<dataset>/date=YYYY-MM-DD/
```

Date partitioning is appropriate for processed telecom activity because:

1. activity is naturally time-based;
2. queries commonly filter by date;
3. Spark can read only relevant partitions;
4. large datasets can be processed incrementally;
5. individual dates can be rebuilt without rewriting the complete dataset.

Static reference data is deliberately not partitioned by date.

---

## 10. Lake / Lakehouse Thinking

The storage design follows a simple lake/lakehouse pattern:

```text
Incoming data
     ↓
  Landing
     ↓
    Raw
     ↓
 Processed
     ↓
 Analytics / Warehouse
```

Reference data supports the processing and analytics layers:

```text
Reference
    ↓
Processed / Analytics
```

Operational information is maintained separately:

```text
Logs → audit and run history
```

CSV is retained where source fidelity and simple interchange are important.

Parquet is used for processed and analytical datasets because it is better suited to large-scale analytical processing.

This provides a balance between auditability, processing efficiency, and query performance.

---

## 11. Storage Contract

The DE5 storage contract is:

1. `data/landing/` stores incoming daily CSV files.
2. `data/raw/` stores accepted source data and is immutable.
3. `data/rejected/` stores records rejected by validation.
4. `data/reference/milano-grid.geojson` stores static geographic reference data.
5. `data/reference/` is explicitly **not date-partitioned**.
6. `data/processed/` stores transformed datasets in Parquet.
7. Processed datasets are date-partitioned using `date=YYYY-MM-DD`.
8. `data/analytics/` stores curated analytical data and warehouse outputs.
9. `logs/` stores pipeline execution and audit history.
10. Raw data uses append-only semantics.
11. Logs use append semantics.
12. Processed data uses append for new dates and controlled date-level overwrite for reruns.
13. Partitioning is applied only where it provides practical processing or query benefits.
14. Retention differs by zone and is based on audit, operational, analytical, and business requirements.

---

## 12. DE5 Acceptance Criteria

* [x] Every zone has a stated format.
* [x] Every zone has a stated write mode.
* [x] Every zone has a stated retention position.
* [x] Reference data is explicitly not date-partitioned.
* [x] `logs/` is explicitly included in the storage strategy.
* [x] Processed data is date-partitioned.
* [x] Raw data is immutable and append-only.
