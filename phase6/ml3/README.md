# ML3 Simple Risk Classifier

ML3 trains an interpretable Logistic Regression baseline from
`phase3/de6/warehouse/network_analytics.db` and the persisted ML2
`network_feature_table`.

The label describes the next hourly interval. The model uses only ML2 features
whose `feature_timestamp` is the last interval visible to the model. The
chronological split is 80% of unique timestamps for training and 20% for test.

Run from the repository root:

```powershell
python -m pytest phase6/ml3/tests/test_training.py -q
python phase6/ml3/train_model.py
```

The evaluation report includes date ranges, accuracy, precision, recall, base
rate, coefficients, and comparison counts against NP3 alerts.
