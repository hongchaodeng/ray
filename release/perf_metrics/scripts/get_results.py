import os
import sys
import re
import json
from typing import Any, Dict, List
import requests
import click


PERF_RESULTS_TO_FETCH = {
    r"^microbenchmark.aws \(.+\)$": "microbenchmark.json",
    r"^many_actors.aws \(.+\)$": "benchmarks/many_actors.json",
    r"^many_nodes.aws \(.+\)$": "benchmarks/many_nodes.json",
    r"^many_pgs.aws \(.+\)$": "benchmarks/many_pgs.json",
    r"^many_tasks.aws \(.+\)$": "benchmarks/many_tasks.json",
    r"^object_store.aws \(.+\)$": "scalability/object_store.json",
    r"^single_node.aws \(.+\)$": "scalability/single_node.json",
    r"^stress_test_dead_actors.aws \(.+\)$": (
        "stress_tests/stress_test_dead_actors.json"
    ),
    r"^stress_test_many_tasks.aws \(.+\)$": "stress_tests/stress_test_many_tasks.json",
    r"^stress_test_placement_group.aws \(.+\)$": (
        "stress_tests/stress_test_placement_group.json"
    ),
}


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
        return builds
    else:
        print(f"Failed to fetch builds: {response.status_code}")
        print(response.text)
        sys.exit(1)


def list_artifacts(branch, commit, build_number, job_id) -> List[Dict[str, Any]]:
    url = (
        "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release"
        f"/builds/{build_number}"
        f"/jobs/{job_id}/artifacts"
    )
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
    response = requests.get(url, headers=headers, params=params)
    if response["status_code"] != 200:
        raise Exception(
            f"Failed to list artifacts: {response['json']}"
        )
    return response["json"]


def download_artifact(branch, commit, build_number, job_id, artifact_id) -> Dict[str, Any]:
    """
    Download artifact from a given artifact ID. The result is in the format:
    - {"type": "<type_of_content>", "content": <content>}
    Type can either be "json" or "text" depends on the artifact file type.
    """
    url = (
        "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release"
        f"/builds/{build_number}"
        f"/jobs/{job_id}"
        f"/artifacts/{artifact_id}"
        "/download"
    )
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
    response = requests.get(url, headers=headers, params=params)
    if response["status_code"] != 200:
        raise Exception(
            f"Failed to list artifacts: {response['json']}"
        )
    content_type = response["headers"].get("Content-Type", "")
    if not content_type:
        raise Exception("Content-Type not found in response headers")
    file_type = content_type.split(";")[0]
    return {"type": file_type, "content": response["content"]}


def find_and_retrieve_artifact_content(branch, commit, build_number, job_id, artifact_filename):
    """Look for artifact file and retrieve its content."""
    artifacts = list_artifacts(branch, commit, build_number, job_id)
    for artifact in artifacts:
        if "filename" not in artifact:
            continue
        if artifact["filename"] == artifact_filename:
            result = download_artifact(
                branch,
                commit,
                build_number,
                job_id,
                artifact["id"],
            )
            return result
    raise Exception(f"Artifact {artifact_filename} not found")

def download_results(branch, commit, builds):
    print("Downloading results")
    perf_results_to_fetch = PERF_RESULTS_TO_FETCH.copy()
    fetched_results = {}
    for build in builds:
        for job in build["jobs"]:
            if "name" in job:
                for job_regex, file_name in list(perf_results_to_fetch.items()):
                    if re.match(job_regex, job["name"]):
                        artifact_content = find_and_retrieve_artifact_content(
                            branch,
                            commit,
                            build["number"],
                            job["id"],
                            "result.json",
                        )
                        fetched_results[file_name] = json.loads(
                            artifact_content["content"].decode()
                        )["results"]
                        perf_results_to_fetch.pop(job_regex)
    for file_name, result in fetched_results:
        print(f"Result({file_name}): {result}")


@click.command()
@click.option("--branch", type=str, default="master")
@click.option("--commit", required=True, type=str)
def main(branch: str, commit: str):
    print(f"Branch: {branch}, Commit: {commit}")
    builds = list_builds(branch, commit)
    download_results(branch, commit, builds)


if __name__ == "__main__":
    main()
