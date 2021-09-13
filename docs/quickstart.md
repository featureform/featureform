# Quick Start

## Step 1: Install Embeddinghub client

Install the Python SDK via pip

```
pip install embeddinghub
```

## Step 2: Deploy Docker container ( _optional_ )
The Embeddinghub client can be used without a server. This is useful when using embeddings in a research environment where a database server is not necessary. If that’s the case for you, skip ahead to the next step.

Otherwise, we can use this docker command to run Embeddinghub locally and to map the container's main port to our host's port.

```
docker run featureformcom/embeddinghub -p 74622:74622
```

## Step 3: Initialize Python Client

If you deployed a docker container, you can initialize the python client.

```py
import embeddinghub as eh

hub = eh.connect(eh.Config())
```
Otherwise, you can use a LocalConfig to store and index embeddings locally.

```py
hub = eh.connect(eh.LocalConfig("data/"))
```

## Step 4: Create a Space

Embeddings are written and retrieved from Spaces. When creating a Space we must also specify a version, otherwise a default version is used.

```py
space = hub.create_space("quickstart", dims=3)
```

## Step 5: Upload Embeddings
We will create a dictionary of three embeddings and upload them to our new quickstart space.

```py
embeddings = {
    "apple": [1, 0, 0],
    "orange": [1, 1, 0],
    "potato": [0, 1, 0],
    "chicken": [-1, -1, 0],
}
space.multiset(embeddings)
```

## Step 6: Get nearest neighbors

Now we can compare apples to oranges and get the nearest neighbors.

```py
neighbors = space.nearest_neighbors(key="apple", num=2)
print(neighbors)
```

## Next Steps
* [Read through our guide to further explore Embeddinghub’s functionality.](overview.md)
* [Join other Embeddinghub users and contributors in our Slack community.](https://featureform-community.slack.com/ssb/redirect)

