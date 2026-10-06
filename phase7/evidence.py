"""
Sample grid evidence objects for Lab C1.

These are fabricated but realistic. Each dict represents ONE grid-cell,
ONE hourly timestamp — the same shape the ML system (Phase 6) would hand
off to Claude. Field meanings:

    grid_id           str   grid cell identifier
    timestamp         str   hourly interval, ISO 8601
    current_activity  float total_activity this hour (network-activity units)
    baseline_activity float expected total_activity for this hour/weekday
    activity_growth   float (current - baseline) / baseline
    peak_ratio        float current_activity / typical daily peak
    variability       float coefficient of variation over trailing window
    internet_share    float fraction of total_activity that is data/internet
    anomaly_score     float 0-1 ML anomaly score (None => deliberately missing)
    direction         str   "up" / "down" / None
    alerts            list  rule-based alerts currently firing

GRID_E deliberately omits anomaly_score/direction/alerts to exercise the
"insufficient evidence" path required by the acceptance criteria.
"""

GRID_A_NORMAL = {
    "grid_id": "G-114",
    "timestamp": "2026-09-10T08:00:00Z",
    "current_activity": 812.4,
    "baseline_activity": 795.0,
    "activity_growth": 0.022,
    "peak_ratio": 0.61,
    "variability": 0.09,
    "internet_share": 0.71,
    "anomaly_score": 0.06,
    "direction": "up",
    "alerts": [],
}

GRID_B_ATTENTION = {
    "grid_id": "G-227",
    "timestamp": "2026-09-10T19:00:00Z",
    "current_activity": 1460.0,
    "baseline_activity": 1120.0,
    "activity_growth": 0.304,
    "peak_ratio": 0.88,
    "variability": 0.22,
    "internet_share": 0.64,
    "anomaly_score": 0.58,
    "direction": "up",
    "alerts": ["growth_above_25pct"],
}

GRID_C_HIGH = {
    "grid_id": "G-309",
    "timestamp": "2026-09-11T21:00:00Z",
    "current_activity": 3120.7,
    "baseline_activity": 1340.0,
    "activity_growth": 1.329,
    "peak_ratio": 1.42,
    "variability": 0.47,
    "internet_share": 0.55,
    "anomaly_score": 0.93,
    "direction": "up",
    "alerts": ["growth_above_25pct", "peak_ratio_exceeded", "variability_spike"],
}

GRID_D_BORDERLINE = {
    "grid_id": "G-158",
    "timestamp": "2026-09-12T03:00:00Z",
    "current_activity": 240.1,
    "baseline_activity": 410.0,
    "activity_growth": -0.414,
    "peak_ratio": 0.19,
    "variability": 0.31,
    "internet_share": 0.48,
    "anomaly_score": 0.51,
    "direction": "down",
    "alerts": ["growth_below_-40pct"],
}

# Same cell as GRID_B, but anomaly_score/direction/alerts withheld on purpose.
GRID_E_INSUFFICIENT = {
    "grid_id": "G-227",
    "timestamp": "2026-09-10T19:00:00Z",
    "current_activity": 1460.0,
    "baseline_activity": 1120.0,
    "activity_growth": 0.304,
    "peak_ratio": 0.88,
    "variability": 0.22,
    "internet_share": 0.64,
    "anomaly_score": None,
    "direction": None,
    "alerts": [],
}

ALL_GRIDS = {
    "A_normal": GRID_A_NORMAL,
    "B_attention": GRID_B_ATTENTION,
    "C_high": GRID_C_HIGH,
    "D_borderline": GRID_D_BORDERLINE,
    "E_insufficient": GRID_E_INSUFFICIENT,
}