Run them from the project root directory.

Setup:

```
mkdir -p tmp\~/perf/
```

Fetch latest performance metrics:

```
python release/perf_metrics_scripts/fetch.py --commit <git-hash> --branch <fork>:<branch> --outputdir tmp\~/perf/ 
```

Convert the metrics to CSV file:

```
python release/perf_metrics_scripts/convert.py
```

