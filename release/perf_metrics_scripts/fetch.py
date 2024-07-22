import os
import sys
import re
import json
from typing import Any, Dict, List, Optional
import requests
import click


PERF_RESULTS_TO_FETCH = {
    r"^microbenchmark.aws \(.+\)$",
    r"^many_actors_smoke_test \(.+\)$",
    r"^many_pgs_smoke_test \(.+\)$",
    r"^many_tasks.aws \(.+\)$",
    r"^object_store.aws \(.+\)$",
    r"^single_node.aws \(.+\)$",
    r"^stress_test_dead_actors.aws \(.+\)$",
    r"^stress_test_many_tasks.aws \(.+\)$",
    r"^stress_test_placement_group.aws \(.+\)$",
}


try:
    buildkite_token = os.environ["BUILDKITE_TOKEN"]
except KeyError:
    print("Environment variable BUILDKITE_TOKEN not found.")
    sys.exit(1)


def list_builds(branch: str, commit: str) -> List[Dict[str, Any]]:
    print("Listing builds")
    url = "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release/builds"
    headers = {
        "Authorization": f"Bearer {buildkite_token}",
        "Content-Type": "application/json",
    }
    params = {
        "commit": commit,
    }
    if commit:
        params["commit"] = commit

    # Make the GET request to the BuildKite API
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch builds: {response.status_code}")

    builds = response.json()
    return builds


def list_artifacts(build_number, job_id) -> List[Dict[str, Any]]:
    print(f"Listing artifacts, {build_number=}, {job_id=}")
    url = (
        "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release"
        f"/builds/{build_number}"
        f"/jobs/{job_id}/artifacts"
    )
    headers = {
        "Authorization": f"Bearer {buildkite_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to list artifacts: {response['json']}")
    return response.json()


def download_artifact(build_number, job_id, artifact_id) -> bytes:
    """
    Download artifact from a given artifact ID. The result is in the format:
    - {"type": "<type_of_content>", "content": <content>}
    Type can either be "json" or "text" depends on the artifact file type.
    """
    print(f"Downloading artifact, {build_number=}, {job_id=}, {artifact_id=}")
    url = (
        "https://api.buildkite.com/v2/organizations/ray-project/pipelines/release"
        f"/builds/{build_number}"
        f"/jobs/{job_id}"
        f"/artifacts/{artifact_id}/download"
    )
    headers = {
        "Authorization": f"Bearer {buildkite_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to download artifact: {response.json()}")
    return response.content


def find_and_retrieve_artifact_content(
    build_number, job_id, artifact_filename
) -> bytes:
    """Look for artifact file and retrieve its content."""
    artifacts = list_artifacts(build_number, job_id)
    for artifact in artifacts:
        if "filename" not in artifact:
            continue
        if artifact["filename"] == artifact_filename:
            result = download_artifact(build_number, job_id, artifact["id"])
            return result
    raise Exception(f"Artifact {artifact_filename} not found")


def fetch_results(builds):
    print("Downloading results")
    perf_results_to_fetch = PERF_RESULTS_TO_FETCH.copy()
    fetched_results = {}

    def on_job_matched(job_regex, build, job):
        jobname = job["name"].split()[0]
        print(f"matched job: {jobname=}")
        artifact_content = find_and_retrieve_artifact_content(
            build["number"],
            job["id"],
            "result.json",
        )
        loaded_content = json.loads(artifact_content.decode())
        fetched_results[jobname] = loaded_content["results"]["perf_metrics"]
        perf_results_to_fetch.remove(job_regex)
        print(f"Fetched {jobname=}")

    for build in builds:
        for job in build["jobs"]:
            if not "name" in job:
                continue
            if job["state"] != "passed":
                continue
            for job_regex in perf_results_to_fetch:
                if re.match(job_regex, job["name"]):
                    on_job_matched(job_regex, build, job)
                    break

    return fetched_results


@click.command()
@click.option("--branch", type=str, default="master")
@click.option("--commit", required=True, type=str)
@click.option("--outputdir", required=True, type=str)
def main(branch: str, commit: str, outputdir: str):
    print(f"Branch: {branch}, Commit: {commit}")
    builds = list_builds(branch, commit)
    results = fetch_results(builds)

    with open(os.path.join(outputdir, "result.json"), "w") as f:
        json.dump(results, f, indent=2)
    print("success!")


if __name__ == "__main__":
    main()
