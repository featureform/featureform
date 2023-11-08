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
from concurrent.futures import ThreadPoolExecutor

client = ff.Client(host="benchmark.featureform.com")


# This is your synchronous function that calls the featureform client
def get_features(client, feature_list, entity_dict):
    return client.features(feature_list, entity_dict)


# This function wraps your synchronous function call in an asynchronous one
async def get_features_async(client, feature_list, entity_dict):
    start_time = datetime.datetime.now()
    result = await asyncio.get_event_loop().run_in_executor(
        None, get_features, client, feature_list, entity_dict
    )
    end_time = datetime.datetime.now()
    return (result, (end_time - start_time).total_seconds())


# This function will run get_features_async and collect completed request counts and times
async def collect_results(client, count_dict, time_list):
    tasks = [
        get_features_async(client, [("feature_0", "v10")], {"entity": ["123"]})
        for _ in range(
            100
        )  # Adjust this range for the number of requests you want to make
    ]
    for future in asyncio.as_completed(tasks):
        (
            _,
            duration,
        ) = await future  # Wait for the request to complete and get the duration
        count_dict["completed_requests"] += 1
        time_list.append(duration)


# This function will print the number of completed requests and the average time each second
async def print_stats_every_second(count_dict, time_list):
    while True:
        await asyncio.sleep(1)  # Wait for one second
        completed = count_dict["completed_requests"]
        if completed > 0:
            average_time = sum(time_list) / completed
        else:
            average_time = 0
        print(
            f"{completed} requests completed in the last second with an average time of {average_time:.4f} seconds."
        )
        count_dict["completed_requests"] = 0  # Reset the count for the next second
        time_list.clear()  # Reset the time list for the next second


# Main coroutine that sets up other coroutines
async def main(client):
    count_dict = {"completed_requests": 0}
    time_list = []
    # Schedule both the collect_results and print_stats_every_second coroutines
    # so they run concurrently
    result_collector = asyncio.create_task(
        collect_results(client, count_dict, time_list)
    )
    stats_printer = asyncio.create_task(print_stats_every_second(count_dict, time_list))

    # Wait for both tasks to complete
    await result_collector
    await stats_printer


# Run the main coroutine
asyncio.run(main(client))
