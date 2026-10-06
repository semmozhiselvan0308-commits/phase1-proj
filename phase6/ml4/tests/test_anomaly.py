import pandas as pd
import pytest

from phase1.np3.network_alert_generator import calculate_bucket_baseline
from phase6.ml4.anomaly import calculate_anomaly_scores


def test_shared_baseline_supports_hour_buckets_and_uses_multiple_days():
    activity = pd.DataFrame(
        {
            "grid_id": [4821, 4821, 4821, 4821],
            "timestamp": pd.to_datetime(
                [
                    "2013-11-01 10:00:00",
                    "2013-11-02 10:00:00",
                    "2013-11-01 11:00:00",
                    "2013-11-02 11:00:00",
                ]
            ),
            "total_activity": [10.0, 30.0, 20.0, 40.0],
        }
    )
    activity["hour_of_day"] = activity["timestamp"].dt.hour

    result = calculate_bucket_baseline(
        activity,
        bucket_key="hour_of_day",
        reducer="median",
        exclude_current=True,
    )

    assert result.loc[0, "baseline_activity"] == pytest.approx(30.0)
    assert result.loc[1, "baseline_activity"] == pytest.approx(10.0)


def test_anomaly_scores_include_high_and_low_direction():
    timestamps = pd.date_range("2013-11-01", periods=72, freq="h")
    values = [10.0] * 24 + [30.0] * 24 + [30.0] * 23 + [1.0]
    activity = pd.DataFrame(
        {
            "grid_id": 4821,
            "timestamp": timestamps,
            "total_activity": values,
        }
    )

    scores = calculate_anomaly_scores(activity, threshold_percent=50)

    assert (scores["direction"] == "HIGH").any()
    assert (scores["direction"] == "LOW").any()
    assert scores["baseline_bucket_count"].max() > 1
    assert scores["reason"].notna().all()
