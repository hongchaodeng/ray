import os
import sys
import json
import click

import pandas as pd

@click.command()
@click.option("--datadir", required=True, type=str)
def main(datadir: str):

    def write_csv(rows, filename):
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(datadir, f'{filename}.csv'), index=False)

    # Load the JSON data
    with open(os.path.join(datadir, 'result.json')) as f:
        data = json.load(f)

    # Divide them into two separate lists: throughput and latency
    tp_rows = []
    lt_rows = []

    # Iterate through the top-level keys and their corresponding lists of metrics
    for job_name, metrics in data.items():
        for metric in metrics:
            name = metric["perf_metric_name"]
            row = {
                "perf_metric_name": f"{job_name}/{name}",
                "perf_metric_value": metric["perf_metric_value"],
            }
            if metric['perf_metric_type'].startswith('THROUGHPUT'):
                tp_rows.append(row)
            elif metric['perf_metric_type'].startswith('LATENCY'):
                lt_rows.append(row)
    write_csv(tp_rows, 'core_throughput')
    write_csv(lt_rows, 'core_latency')


    tp_rows = []
    lt_rows = []
    
    with open(os.path.join(datadir, 'serve.json')) as f:
        data = json.load(f)
    for job_name, metrics in data.items():
        for metric in metrics:
            name = metric["perf_metric_name"]
            row = {
                "perf_metric_name": f"{job_name}/{name}",
                "perf_metric_value": metric["perf_metric_value"],
            }
            if metric['perf_metric_type'].startswith('THROUGHPUT'):
                tp_rows.append(row)
            elif metric['perf_metric_type'].startswith('LATENCY'):
                lt_rows.append(row)
    write_csv(tp_rows, 'serve_throughput')
    write_csv(lt_rows, 'serve_latency')

    tp_rows = []
    lt_rows = []
    
    with open(os.path.join(datadir, 'data.json')) as f:
        data = json.load(f)

    for job_name, results in data.items():
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

    write_csv(tp_rows, 'data_throughput')
    write_csv(lt_rows, 'data_latency')

if __name__ == "__main__":
    main()
