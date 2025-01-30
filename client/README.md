# Featureform Python Client

## Overview

Featureform’s Python client is a SDK for defining, managing and serving resources (e.g. infrastructure providers, data sources, transformations, etc.). At a high level, the API is divided into two parts:

* Registration: register data stores (e.g. PostgreSQL), data sources (e.g. tables or CSV files) as resources or get and/or list previously registered resources
* Serving: retrieve training sets and features for offline training or online inference

## Requirements

* Python 3.9-3.12

## Setting Up Your Local Development Environment

### Step 1: Install gRPC and Protocol Buffer Tooling

See grpc.io for instructions on installing the [protocol buffer compiler](https://grpc.io/docs/protoc-installation/) for your OS and language-specific plugins for [Golang](https://grpc.io/docs/languages/go/quickstart/#prerequisites) (**NOTE**: the Golang dependencies can also be installed via [Homebrew](https://brew.sh/).)

### Step 2: Create Python Virtual Environment

You may create a Python virtual environment however you prefer, but the directory name `.venv` is ignored by Git for convenience, so you may choose to create your virtual environment in the root of the project.

```shell
> python -m venv .venv && . .venv/bin/activate
(.venv) >
```

### Step 3: Upgrade pip and Install Build Dependencies

The following dependencies are required to build the client:

* [build](https://pypi.org/project/build/)
* [pytest](https://pypi.org/project/pytest/)
* [python-dotenv](https://pypi.org/project/python-dotenv/)
* [pytest-mock](https://pypi.org/project/pytest-mock/)

```shell
(.venv) > python -m pip install --upgrade pip
(.venv) > python -m pip install -r requirements.txt
```

### Step 4: Compile API Protocol Buffers and Python Stub

The shell script `gen_grpc.sh` has been provided for convenience. Change the file access permissions to make it executable and run it:

```shell
(.venv) > chmod +x gen_grpc.sh
(.venv) > ./gen_grpc.sh
```

### Step 5: Build Python Client and Dashboard

The shell script `pip_update.sh` has been provided for convenience. Change the file access permissions to make it executable and run it:

```shell
(.venv) > chmod +x pip_update.sh
(.venv) > ./pip_update.sh
```

### Step 6: Optionally Run Client Test Suite

To ensure your changes haven’t broken the client, run the test suite with the following make target:

```shell
(.venv) > make pytest
```

## Outcome

With steps 1-5 successfully completed, you should have the `featureform` CLI command accessible in your terminal session.

```shell
(.venv) > featureform -h
```

To further verify that your setup is complete and correct, you may optionally walk through the [Quickstart](https://docs.featureform.com/quickstart-local) tutorial. You may put the `definitions.py` file at the root of the project, which won’t be ignored by Git, or use a URL to a file (e.g. hosted on GitHub).
