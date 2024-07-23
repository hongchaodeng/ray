import os
import sys
import re
import json
from typing import Any, Dict, List, Optional
import requests
import click


PERF_RESULTS_TO_FETCH = {
    r"^many_(.+)$",
    r"^object_store.aws (.+)$",
    r"^single_node.aws (.+)$",
    r"^agent_stress_test.aws (.+)$",
    r"^stress_test_(.+)$",
    r"^microbenchmark.aws (.+)$",
    r"^serve_microbenchmarks.aws (.+)$",
    r"serve_autoscaling_load_test.aws (.+)$",
    r"autoscaling_shuffle_1tb_1000_partitions.aws (.+)$",
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
    fetched_results = {}

    def download_perf_metrics(build, job):
        jobname = job["name"].split()[0]
        print(f"matched job: {jobname=}")
        artifact_content = find_and_retrieve_artifact_content(
            build["number"],
            job["id"],
            "result.json",
        )
        loaded_content = json.loads(artifact_content.decode())
        if "results" in loaded_content and "perf_metrics" in loaded_content["results"]:
            fetched_results[jobname] = loaded_content["results"]["perf_metrics"]
            print(f"Fetched {jobname=}")
        else:
            print(f"Failed to fetch {jobname=}")

    for build in builds:
        for job in build["jobs"]:
            if not "name" in job:
                continue
            if job["state"] != "passed":
                continue
            # print("job: ", job["name"])
            for regex in PERF_RESULTS_TO_FETCH:
                if re.match(regex, job["name"]):
                    download_perf_metrics(build, job)
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
