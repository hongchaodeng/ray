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
    r"^stress_test_(.+)$",
    r"^microbenchmark.aws (.+)$",
    r"autoscaling_shuffle_1tb_1000_partitions.aws (.+)$",
}

SERVE_RESULTS_TO_FETCH = {
    r"^serve_microbenchmarks.aws (.+)$",
    r"serve_autoscaling_load_test.aws (.+)$",
}

# Data + Train
DATA_RESULTS_TO_FETCH = {
    r"^aggregate_benchmark.aws (.+)$",
    r"^read_images_benchmark_single_node.aws (.+)$",
    r"^read_images_comparison_microbenchmark_single_node.aws (.+)$",
    r"^read_tfrecords_benchmark_single_node.aws (.+)$",
    r"^read_parquet_benchmark_single_node.aws (.+)$",
    r"^parquet_metadata_resolution.aws (.+)$",
    r"^streaming_data_ingest_benchmark_(.+)$",
    r"^iter_batches_benchmark_single_node.aws (.+)$",
    r"^iter_tensor_batches_benchmark_single_node.aws (.+)$",
    r"^map_batches_benchmark_single_node.aws (.+)$",
    r"^iter_tensor_batches_benchmark_multi_node.aws (.+)$",
    r"^read_images_train_4_gpu.aws (.+)$",
    r"^read_images_train_16_gpu.aws (.+)$",
    r"^read_images_train_16_gpu_preserve_order.aws (.+)$",
    r"^read_parquet_train_4_gpu.aws (.+)$",
    r"^read_parquet_train_16_gpu.aws (.+)$",
    r"^dataset_shuffle_random_shuffle_1tb.aws (.+)$",
    r"^torch_batch_inference_1_gpu_10gb_parquet.aws (.+)$",
    r"^torch_batch_inference_16_gpu_300gb_raw.aws (.+)$",
    r"^torch_batch_inference_16_gpu_300gb_parquet.aws (.+)$",
    r"^stable_diffusion_benchmark.aws (.+)$",
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


def fetch_results(build):
    print("Downloading results")
    perf_results = {}
    serve_results = {}
    data_results = {}

    def handle_perf_metrics(jobname, content):
        if "results" in content and "perf_metrics" in content["results"]:
            perf_results[jobname] = content["results"]["perf_metrics"]
            return True
        return False
    
    def handle_data_metrics(jobname, content):
        if "results" in content:
            data_results[jobname] = content["results"]
            return True
        return False
    
    def handle_serve_metrics(jobname, content):
        if "results" in content and "perf_metrics" in content["results"]:
            serve_results[jobname] = content["results"]["perf_metrics"]
            return True
        return False

    def download_perf_metrics(build, job, on_fetched):
        jobname = job["name"].replace(" ", "_")
        print(f"matched job: {jobname=}")
        artifact_content = find_and_retrieve_artifact_content(
            build["number"],
            job["id"],
            "result.json",
        )
        loaded_content = json.loads(artifact_content.decode())
        if on_fetched(jobname, loaded_content):
            print(f"Fetched {jobname=}")
        else:
            print(f"Failed to fetch {jobname=}")

    for job in build["jobs"]:
        if not "name" in job:
            continue
        if job["state"] != "passed":
            continue
        # print("job: ", job["name"])
        found = False
        # core
        for regex in PERF_RESULTS_TO_FETCH:
            if re.match(regex, job["name"]):
                download_perf_metrics(build, job, handle_perf_metrics)
                found = True
                break
        if found:
            continue
        # data
        for regex in DATA_RESULTS_TO_FETCH:
            if re.match(regex, job["name"]):
                download_perf_metrics(build, job, handle_data_metrics)
                found = True
                break
        if found:
            continue
        # serve
        for regex in SERVE_RESULTS_TO_FETCH:
            if re.match(regex, job["name"]):
                download_perf_metrics(build, job, handle_serve_metrics)
                found = True
                break

    return perf_results, serve_results, data_results


@click.command()
@click.option("--branch", type=str, default="master")
@click.option("--commit", required=True, type=str)
@click.option("--outputdir", required=True, type=str)
def main(branch: str, commit: str, outputdir: str):
    print(f"Branch: {branch}, Commit: {commit}")
    builds = list_builds(branch, commit)
    perf_results, serve_results, data_results = fetch_results(builds[0])

    with open(os.path.join(outputdir, "result.json"), "w") as f:
        json.dump(perf_results, f, indent=2)
    with open(os.path.join(outputdir, "serve.json"), "w") as f:
        json.dump(serve_results, f, indent=2)
    with open(os.path.join(outputdir, "data.json"), "w") as f:
        json.dump(data_results, f, indent=2)
    print("success!")


if __name__ == "__main__":
    main()
