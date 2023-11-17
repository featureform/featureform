import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, asdict

import numpy as np
from tabulate import tabulate

import featureform as ff

logging.basicConfig(
    filename="benchmarking_error.log",
    filemode="w",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

client = ff.Client(host="benchmark.featureform.com")


async def get_features_async(features):
    try:
        start_time = time.perf_counter()

        # Wrap the synchronous get_features call in asyncio.to_thread
        await asyncio.to_thread(get_features, features)

        end_time = time.perf_counter()
        return (end_time - start_time) * 1000  # Return the latency
    except Exception as e:
        logging.error("Error in get_features_async", exc_info=True)
        raise e


def get_features(features):
    try:
        return client.features(features, {"entity": ["9119"]})
    except Exception as e:
        logging.error(e, exc_info=True)


def build_features(feature_count):
    features = []
    for i in range(feature_count):
        features.append((f"feature_{i}", "v10"))
    return features


async def schedule_calls(feature_count, rps, duration=60):
    tasks = []
    interval = 1 / rps  # Interval between each call to maintain the desired RPS
    total_requests = int(rps * duration)  # Total number of requests to make

    print(f"Making {total_requests} requests at {rps} RPS for {feature_count} features")

    for i in range(total_requests):
        features = build_features(feature_count)
        task = asyncio.create_task(get_features_async(features))
        # print("fired off task: ", i)
        tasks.append(task)
        await asyncio.sleep(interval)

    return await asyncio.gather(*tasks, return_exceptions=True)


async def gather_stats(feature_count, rps, duration):
    latencies = await schedule_calls(feature_count, rps, duration)

    # Process the latencies to compute stats
    valid_latencies = [
        latency for latency in latencies if not isinstance(latency, Exception)
    ]
    errors = len(latencies) - len(valid_latencies)

    avg_latency = sum(valid_latencies) / len(valid_latencies) if valid_latencies else 0
    min_latency = min(valid_latencies) if valid_latencies else 0
    max_latency = max(valid_latencies) if valid_latencies else 0

    p50 = np.percentile(valid_latencies, 50) if valid_latencies else 0
    p90 = np.percentile(valid_latencies, 90) if valid_latencies else 0
    p95 = np.percentile(valid_latencies, 95) if valid_latencies else 0
    p99 = np.percentile(valid_latencies, 99) if valid_latencies else 0

    return Stats(
        avg_latency=avg_latency,
        min_latency=min_latency,
        max_latency=max_latency,
        p50=p50,
        p90=p90,
        p95=p95,
        p99=p99,
        errors=errors,
    )


@dataclass
class Stats:
    avg_latency: float
    min_latency: float
    max_latency: float
    p50: float
    p90: float
    p95: float
    p99: float
    errors: int


@dataclass
class FeatureServingRun:
    features: int
    rps: int
    stats: Stats


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration", type=int, default=60, help="Duration for the benchmark in seconds"
    )
    args = parser.parse_args()

    duration = args.duration

    feature_counts = [1]
    rps_values = [10]

    runs = []

    # Print Table Header
    headers = [
        "features",
        "rps",
        "avg_latency",
        "min_latency",
        "max_latency",
        "p50",
        "p90",
        "p95",
        "p99",
        "errors",
    ]
    print(tabulate([], headers=headers, tablefmt="plain"))

    for rps in rps_values:
        for feature_count in feature_counts:
            stats = await gather_stats(feature_count, rps, duration)
            run = FeatureServingRun(feature_count, rps, stats)

            # Convert the run to a dictionary and flatten stats
            run_dict = asdict(run)
            stats_dict = run_dict.pop("stats")

            # Format the stats values
            formatted_stats = {k: f"{v:.2f}" for k, v in stats_dict.items()}

            # Combine the dictionaries
            run_dict.update(formatted_stats)
            runs.append(run_dict)

            # Print each row as it's ready
            # print(tabulate([run_dict.values()], headers="", tablefmt="plain"))

    # # Convert dataclass instances to list of dictionaries
    # table_data = [asdict(run) for run in runs]
    #
    # # Flatten the stats dictionary into the main dictionary
    # for item in table_data:
    #     item.update(item.pop("stats"))

    # Display table
    print(tabulate(runs, headers="keys", tablefmt="pretty"))


if __name__ == "__main__":
    asyncio.run(main())
