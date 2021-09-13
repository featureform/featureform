# Deploying with Docker

## How to use this image

Embeddinghub is quite easy to deploy and use with Docker. It will listen on port 74622 by default. This can be mapped to the host port using the docker CLI.

```console
docker run -d -p 74622:74622 featureformcom/embeddinghub
```

Embeddinghub writes to ~/.embeddinghub/data by default. Docker's filesystem is not optimized for heavy write and read workloads. To increase performance, the data directory that Embeddinghub writes to should be mapped to a docker volume.

```console
docker run -d -v /custom/mount:/root/.embeddinghub/data -p 74622:74622 featureformcom/embeddinghub
```

To interact with the docker container, install the python library using pip.

```console
pip install embeddinghub
```

Afterwards, we can connect to the instance and start reading and writing to it.

```py
import embeddinghub as eh

hub = eh.connect(eh.Config(host="0.0.0.0", port=74622))
space = hub.create_space("test_space", 3)
space.set("key", [1, 2, 3])
```

## How to extend this image

Embeddinghub supports a few different environmental variables that can be used to configure its behavior.

### Environmental Variables

#### EMBEDDINGHUB_PORT

By default, Embeddinghub listens on port 74622, but this can be overrided by setting EMBEDDINGHUB_PORT.

```console
docker run -d -e EMBEDDINGHUB_PORT=7000 -p 7000:7000 featureformcom/embeddinghub
```

#### EMBEDDINGHUB_DATA

By default, Embeddinghub writes all user data to ~/.embeddinghub/data. On the alpine linux base image, this means it writes to /root/.embeddinghub/data. This can be overrided by setting the EMBEDDINGHUB_DATA flag.

```console
docker run -d -e EMBEDDINGHUB_DATA=/embeddinghub -v /custom/mount:/embeddinghub -p 74622:74622 featureformcom/embeddinghub
```
