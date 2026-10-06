# ML4 Anomaly Baseline

ML4 compares each grid-hour with the historical median for the same hour of
day. The current row is excluded from its baseline. A 50% absolute percentage
deviation is flagged, with HIGH and LOW direction retained.

Run from the repository root:

```powershell
python -m pytest phase6/ml4/tests/test_anomaly.py -q
python phase6/ml4/anomaly.py
```

The script persists `network_anomaly_scores` in the warehouse and writes the
three-way comparison and anomaly report under `phase6/ml4/outputs`.
