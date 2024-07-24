import os
import sys
import json
import click

import pandas as pd

@click.command()
@click.option("--datadir", required=True, type=str)
def main(datadir: str):
    # Load the JSON data
    with open(os.path.join(datadir, 'result.json')) as f:
        data = json.load(f)

    # Prepare a list to hold the rows for the DataFrame
    rows = []

    # Iterate through the top-level keys and their corresponding lists of metrics
    for job_name, metrics in data.items():
        for metric in metrics:
            row = {}
            for key, value in metric.items():
                if key == "perf_metric_name":
                    row[key] = f"{job_name}/{value}"
                else:
                    row[key] = value
            rows.append(row)

    # Divide them into two separate lists: throughput and latency
    tp_rows = []
    lt_rows = []
    
    for row in rows:
        r = {k: v for k, v in row.items() if k != 'perf_metric_type'}
        if row['perf_metric_type'].startswith('THROUGHPUT'):
            tp_rows.append(r)
        elif row['perf_metric_type'].startswith('LATENCY'):
            lt_rows.append(r)

    with open(os.path.join(datadir, 'data.json')) as f:
        data = json.load(f)

    for job_name, results in data.items():
        if "perf_metrics" in results:
            for name, value in results["perf_metrics"].items():
                row = {
                    "perf_metric_name": f"{job_name}/{name}",
                    "perf_metric_value": value["THROUGHPUT"],
                }
                tp_rows.append(row)
            continue
        
        if "name" not in results:
            continue
        name = results["name"]
        
        for key in results:
            if key == "name":
                continue
            value = results[key]
            if "time" in value:
                row = {
                    "perf_metric_name": f"{job_name}/{name}-{key}",
                    "perf_metric_value": value["time"],
                }
                lt_rows.append(row)

            if "tput" in value:
                row = {
                    "perf_metric_name": f"{job_name}/{name}-{key}",
                    "perf_metric_value": value["tput"],
                }
                tp_rows.append(row)

    def write_csv(rows, filename):
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(datadir, f'{filename}.csv'), index=False)

    write_csv(tp_rows, 'throughput')
    write_csv(lt_rows, 'latency')

if __name__ == "__main__":
    main()
