# import featureform as ff
# import datetime
#
# client = ff.Client(host="benchmark.featureform.com")
#
# for i in range(0, 100):
#     start = datetime.datetime.now()
#     feat = client.features([("feature_0", "v10")], {"entity": ["123"]})
#     print(feat)
#     print(datetime.datetime.now() - start)

import asyncio
import datetime
import featureform as ff
import random
from concurrent.futures import ThreadPoolExecutor

# Set up the client (you will need to replace this with actual initialization)
client = ff.Client(host="benchmark.featureform.com")

# Configure your desired rate limit
requests_per_second = 50  # The number of requests you want to allow per second
rate_limit = asyncio.Semaphore(requests_per_second)  # Semaphore for rate limiting


# Function to fetch features
def get_features(client, feature_list, entity_dict):
    # Here you would normally handle exceptions and other logic
    return client.features(feature_list, entity_dict)


# Async wrapper function to call the synchronous get_features function
async def get_features_async(client, feature_list, entity_dict, executor):
    async with rate_limit:
        start_time = datetime.datetime.now()
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                executor, get_features, client, feature_list, entity_dict
            )
            success = True
        except Exception as e:
            result = None
            success = False
        end_time = datetime.datetime.now()
        return (result, success, (end_time - start_time).total_seconds())


# Coroutine to collect results from get_features_async calls
async def collect_results(client, count_dict, time_list, failed_count, executor):
    while True:
        feat = random.randint(0, 249)
        task = get_features_async(
            client, [(f"feature_{feat}", "v10")], {"entity": ["123"]}, executor
        )
        _, success, duration = await task
        if success:
            count_dict["completed_requests"] += 1
            time_list.append(duration)
        else:
            failed_count["failures"] += 1


# Coroutine to print stats every second
async def print_stats_every_second(count_dict, time_list, failed_count):
    while True:
        await asyncio.sleep(1)  # Sleep for one second
        completed = count_dict["completed_requests"]
        failures = failed_count["failures"]
        if completed > 0:
            average_time = sum(time_list) / completed
        else:
            average_time = 0
        print(
            f"{completed} successful requests and {failures} failed requests in the last second. Average time: {average_time:.4f} seconds."
        )
        # Reset the counters and the time list
        count_dict["completed_requests"] = 0
        failed_count["failures"] = 0
        time_list.clear()


# Main coroutine to set up and run the collector and printer coroutines
async def main(client):
    count_dict = {"completed_requests": 0}
    failed_count = {"failures": 0}
    time_list = []

    with ThreadPoolExecutor(max_workers=requests_per_second) as executor:
        collector = asyncio.create_task(
            collect_results(client, count_dict, time_list, failed_count, executor)
        )
        printer = asyncio.create_task(
            print_stats_every_second(count_dict, time_list, failed_count)
        )

        # You can adjust the duration for how long you want the printer to run
        await asyncio.sleep(10)  # For example, run for 10 seconds
        collector.cancel()  # Cancel the collector after the duration ends

        # Await the collector to allow for any final tasks to complete
        try:
            await collector
        except asyncio.CancelledError:
            pass  # Task was cancelled, which is expected


# Run the main coroutine
asyncio.run(main(client))
