# System Architecture

![A map of the Featureform services and the components that they interface with.](.gitbook/assets/Featureform-Arch-Lucidchart.png)

## Kubernetes

Featureform runs on Kubernetes. We use it to run our services, and to spin up worker pods. Kubernetes allows us to be cloud-agnostic, it also makes it easy to deploy us. It's just a one-line helm install. Note that even though Featureform requires Kubernetes, your infrastructure providers do not have to be running on Kubernetes.

## Metadata

The metadata contains all of the definitions a user creates via the Python interface. It is the source of truth and is strongly consistent. It acts as the "desired" state for the system, and the coordinator's job is to achieve that state. It uses Etcd as a backing store.

## Coordinator

The coordinator service listens for changes in metadata and then creates worker pods to interact with the infrastructure providers. The workers actually perform work like copying data between places, whereas the coordinator handles failure, retrys, and other distributed system logic. It also makes sure that operations are performed atomically. Finally, it handles scheduling for transformations that run on a cadence.

## Serving

Serving directly interacts with infrastructure providers. It maps the Featureform abstraction to the underlying tables that physically make up each feature and training set. It aims to be as lightweight as possible to add as little latency as possible.

## Monitoring

Prometheus is configured to monitor Featureform itself as well as the infrastructure providers. This information is used directly in the dashboard and can be queried directly.

## Dashboard

The dashboard is a read-only view on the metadata and metrics. It interacts with a dashboard API which wraps metadata, and directly with Prometheus for monitoring data. The dashboard itself is written in React.
