import os
import sys
import requests
import click

try:
  buildkite_token = os.environ['BUILDKITE_TOKEN']
except KeyError:
  print('Environment variable BUILDKITE_TOKEN not found.')
  sys.exit(1)
    
def list_builds(branch: str, commit: str):
  print("Listing builds")
  url = "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release/builds"
  headers = {
    'Authorization': f'Bearer {buildkite_token}',
    'Content-Type': 'application/json',
  }
  params = {
    "branch": branch,
    "commit": commit,
    # "state": "passed",
    # "created_from": "2024-07-13T23:34:38Z", # ISO 8601 format
  }

  # Make the GET request to the BuildKite API
  response = requests.get(url, headers=headers, params=params)

  if response.status_code == 200:
    # Parse the JSON response
    builds = response.json()
    # Print the pipelines
    for build in builds:
        print(f"build: {build['number']}, state: {build['state']}, created_at: {build['created_at']}")
  else:
    print(f"Failed to fetch builds: {response.status_code}")
    print(response.text)
    sys.exit(1)

def download_results():
  print("Downloading results")

@click.command()
@click.option("--branch", type=str, default="master")
@click.option("--commit", required=True, type=str)
def main(branch: str, commit: str):
  print(f"Branch: {branch}, Commit: {commit}")
  list_builds(branch, commit)
  download_results()

if __name__ == "__main__":
  main()
