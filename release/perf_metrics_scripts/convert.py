import pandas as pd
import json

# Load the JSON data
with open('tmp~/perf/result.json') as f:
    data = json.load(f)

# Prepare a list to hold the rows for the DataFrame
rows = []

# Iterate through the top-level keys and their corresponding lists of metrics
for job_name, metrics in data.items():
    for metric in metrics:
        row = {}
        for key, value in metric.items():
            if key == "perf_metric_name":
                row[key] = f"{job_name}-{value}"
            else:
                row[key] = value
        rows.append(row)

# Convert the list of rows to a DataFrame
df = pd.DataFrame(rows)

# Save DataFrame to CSV
df.to_csv('tmp~/perf/output.csv', index=False)
