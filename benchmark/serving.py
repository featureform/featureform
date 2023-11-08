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
import asyncio
import datetime
from concurrent.futures import ThreadPoolExecutor

client = ff.Client(host="benchmark.featureform.com")


# Assuming client.features is your synchronous function
def get_features(client, feature_list, entity_dict):
    # Your synchronous code to get features
    return client.features(feature_list, entity_dict)


async def get_features_async(client):
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                get_features,
                client,
                [(f"feature_{i}", "v10")],
                {"entity": ["123"]},
            )
            for i in range(100)
        ]
        for future in asyncio.as_completed(futures):
            start = datetime.datetime.now()
            feat = await future
            print(feat)
            print(datetime.datetime.now() - start)


# Run the asynchronous function
asyncio.run(get_features_async(client))


# Assuming 'client' is defined elsewhere and is the object that has the `features()` method.
# Run the async function
asyncio.run(get_features_async(client))
