# DE4 — Batch vs Streaming Decision Workshop

## 1. Objective

The purpose of this workshop is to make architecture decisions based on business latency requirements, data arrival patterns, cost, and operational complexity.

The key decision is whether each telecom workload should use **batch processing** or **streaming processing**.

---

## 2. Batch vs Streaming Decision Matrix

| Workload                          | Source                           | Arrival Pattern           |                   Required Latency | Decision                                                          | Justification                                                                                       |
| --------------------------------- | -------------------------------- | ------------------------- | ---------------------------------: | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Daily usage summary               | Daily telecom activity CSV files | Once per day              |                    Within 24 hours | **Batch**                                                         | The summary is a daily business output. Real-time processing provides little additional value.      |
| Hypothetical live activity events | Live network events              | Continuous, seconds apart |                        1–5 seconds | **Streaming**                                                     | Events need to be processed almost immediately.                                                     |
| Billing report                    | Processed usage and billing data | Daily/monthly schedule    |                  Within 1–24 hours | **Batch**                                                         | Billing is periodic and does not require second-level processing.                                   |
| Hotspot alerts                    | Live network activity events     | Continuous                |                       5–60 seconds | **Streaming**                                                     | Hotspot conditions need rapid detection so that operations can respond quickly.                     |
| Executive dashboard refresh       | Aggregated analytics data        | Every 15–60 minutes       |                      15–60 minutes | **Batch / Micro-batch**                                           | Periodic dashboard updates are sufficient and avoid unnecessary streaming complexity.               |
| Model training                    | Historical processed data        | Scheduled daily/weekly    |                  Within 1–24 hours | **Batch**                                                         | Model training operates on historical datasets and does not require event-by-event processing.      |
| Model scoring                     | New activity/events              | Batch or continuous       | 1–60 seconds for real-time scoring | **Streaming when real-time scoring is required; batch otherwise** | Live operational predictions may require immediate scoring, while offline scoring can remain batch. |

---

## 3. Why the Training Dataset Uses Batch Processing

Telecom network activity is continuous in the real world. However, the training dataset used in this project is not a live event stream.

The project receives telecom activity as **daily CSV files containing hourly observations**.

The current architecture is:

```text
Daily Telecom CSV Files
          |
          v
    Spark Batch ETL
          |
          v
   Processed Activity
          |
          v
       Analytics
          |
          v
 Dashboard / Reports
```

The architecture should reflect the actual source and arrival pattern of the project rather than pretending that the supplied CSV files are live events.

A streaming architecture would only be justified if the source actually produced continuous events and the business required low-latency processing.

---

## 4. Batch vs Streaming Trade-offs

| Factor                     | Batch                                        | Streaming                                    |
| -------------------------- | -------------------------------------------- | -------------------------------------------- |
| Processing model           | Scheduled groups of data                     | Continuous event processing                  |
| Typical latency            | Minutes to days depending on schedule        | Seconds to minutes                           |
| Infrastructure complexity  | Lower                                        | Higher                                       |
| Operational cost           | Lower                                        | Higher                                       |
| Monitoring requirements    | Simpler                                      | More complex                                 |
| Failure handling           | Generally simpler                            | More complex                                 |
| Best suited for            | Reports, historical analysis, model training | Alerts, live monitoring, real-time scoring   |
| Training dataset           | **Appropriate**                              | **Not necessary**                            |
| Future live network events | Possible                                     | **Appropriate when low latency is required** |

The decision should not be based on choosing the newest technology. The business requirement should determine the architecture.

---

## 5. Where Kafka Could Enter the Architecture

Kafka could conceptually be introduced in a future production environment where live telecom network events are available.

Kafka would sit between the live event producers and the streaming processing layer.

The existing training dataset does not need to be changed.

```text
                         TELECOM DATA SOURCES
                                  |
                   +--------------+--------------+
                   |                             |
                   v                             v
           Daily CSV Files              Live Network Events
                   |                             |
                   v                             v
            Spark Batch                       Kafka
                   |                             |
                   |                             v
                   |                     Stream Processing
                   |                         /       \
                   |                        /         \
                   |                       v           v
                   |                Hotspot Alerts   Real-time
                   |                              Model Scoring
                   |
                   +------------------+------------------+
                                      |
                                      v
                              Analytics / Dashboard
                                      |
                                      v
                                Model Training
```

### Important

**The Kafka and streaming path is OPTIONAL and NOT BUILT in this training project.**

The existing batch pipeline remains unchanged.

---

## 6. Model Training vs Real-Time Model Scoring

### 6.1 Model Training — Batch

Model training is suitable for batch processing because it uses historical data.

```text
Historical Activity Data
          |
          v
   Data Preparation
          |
          v
  Feature Engineering
          |
          v
    Model Training
          |
          v
      Trained Model
```

Training can be scheduled daily, weekly, or whenever sufficient new historical data becomes available.

There is no requirement to train the model for every individual network event.

Therefore, **batch processing is the appropriate choice for model training**.

---

### 6.2 Real-Time Model Scoring — Potential Streaming

A production system could use streaming when predictions are required immediately.

```text
Live Network Event
        |
        v
      Kafka
        |
        v
 Stream Processing
        |
        v
 Feature Calculation
        |
        v
   Trained Model
        |
        v
 Real-time Prediction
        |
        v
 Alert / Business Action
```

This approach could support real-time operational use cases where a prediction is required within seconds.

This is a **potential future architecture** and is not implemented in this training project.

---

## 7. Defended Architecture Decision — Daily Usage Summary

### Decision

**Daily usage summary → Batch**

### Defence

Batch is the better engineering choice because **streaming would add cost without adding value here**.

The input data arrives as daily CSV files, and the business output is a daily usage summary.

There is no requirement to update the summary every second.

Using Kafka and continuous stream processing would introduce additional:

* Infrastructure
* Monitoring
* Deployment complexity
* Failure-handling requirements
* Operational maintenance
* Cost

without providing meaningful additional business value.

Therefore:

```text
Daily CSV Files
      |
      v
 Spark Batch
      |
      v
Daily Usage Summary
```

is the simpler and more appropriate engineering solution.

---

## 8. Defended Hypothetical Streaming Decision — Hotspot Alerts

### Decision

**Hotspot alerts → Streaming**

Hotspot alerts are a suitable candidate for streaming if live network events are available.

A hotspot can develop quickly, so waiting until the next daily batch could be too slow for an operational response.

A possible architecture is:

```text
Live Network Activity
          |
          v
        Kafka
          |
          v
  Stream Processing
          |
          v
 Detect High Activity
          |
          v
    Hotspot Alert
```

A target latency of **5–60 seconds** is appropriate for this hypothetical operational use case.

The important assumption is that the production system has access to continuous live network events.

---

## 9. Architecture Assumptions

The decisions above depend on several assumptions:

1. The current training data arrives as daily CSV files.
2. The CSV files contain hourly telecom activity observations.
3. Daily usage summaries do not require second-level updates.
4. Billing reports are periodic business outputs.
5. Executive dashboards can tolerate periodic refreshes.
6. Model training operates on historical data.
7. Real-time model scoring would only require streaming if immediate predictions have business value.
8. Hotspot alerts would require live network events in a production environment.
9. Kafka would only be introduced if a continuous event source exists.
10. The optional streaming architecture is not implemented in this project.

---

## 10. Adversarial Review

The architecture should be reviewed critically rather than assuming that streaming is always better.

Important questions for an adversarial review include:

* Does the dashboard actually need second-level updates?
* Does hotspot detection genuinely require streaming?
* Could micro-batch processing satisfy the requirement?
* Is Kafka justified by the expected event volume?
* What is the cost of operating Kafka and a streaming pipeline?
* What additional monitoring would streaming require?
* How would duplicate or delayed events be handled?
* What happens if the streaming system becomes unavailable?
* Does real-time model scoring provide enough business value to justify its complexity?
* Could some workloads remain batch even if live data is available?

The main architectural principle is:

> **Choose streaming when the business requires low latency, not simply because the source data is continuous.**

---

## 11. Final Revised Architecture

The final architecture keeps the existing batch pipeline and adds a conceptual optional streaming path.

```text
                              TELECOM SOURCES
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v
            Daily CSV Files                Live Network Events
                    |                               |
                    v                               v
             +-------------+                 +-------------+
             | Spark Batch |                 |    Kafka    |
             +------+------+                 +------+------+
                    |                               |
                    |                               v
                    |                       +---------------+
                    |                       | Stream Process|
                    |                       +-------+-------+
                    |                               |
                    |                       +-------+-------+
                    |                       |               |
                    |                       v               v
                    |                 Hotspot Alerts   Real-time
                    |                                 Model Scoring
                    |
                    +---------------+-----------------------+
                                    |
                                    v
                           Analytics / Dashboard
                                    |
                                    v
                             Model Training
```

### Streaming Path Status

**OPTIONAL STREAMING PATH — NOT BUILT**

Kafka and stream processing are shown only as a possible future production architecture. The current training project continues to use the supplied daily CSV files and Spark batch processing.

---

## 12. Final Decision Summary

| Decision                          | Final Choice                                                           |
| --------------------------------- | ---------------------------------------------------------------------- |
| Daily usage summary               | **Batch**                                                              |
| Hypothetical live activity events | **Streaming**                                                          |
| Billing report                    | **Batch**                                                              |
| Hotspot alerts                    | **Streaming**                                                          |
| Executive dashboard refresh       | **Batch / Micro-batch**                                                |
| Model training                    | **Batch**                                                              |
| Real-time model scoring           | **Streaming when real-time predictions are required; otherwise batch** |
| Kafka                             | **Optional future component — not built**                              |

---

## 13. DE4 Acceptance Criteria

* [x] Every row of the decision matrix records required latency as a number or range.
* [x] At least one decision is explicitly **batch because streaming would add cost without adding value here**.
* [x] The revised architecture shows the streaming path as **optional and unbuilt**.
* [x] A batch choice is defended.
* [x] A hypothetical streaming choice is defended.
* [x] Kafka's conceptual position is identified.
* [x] Batch is mapped to model training.
* [x] Streaming is mapped to potential real-time model scoring.
* [x] The training dataset is not changed.
* [x] Cost and operational complexity are considered as architecture criteria.

---

# DE4 Status

**COMPLETE**
