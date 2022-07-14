# Minikube

In this deployment, we'll setup a simple Minikube cluster and deploy Postgres and Redis within it. Then we will deploy Featureform on the Minikube deployment and configure it with Postgres as an Offline Store and Redis as an Inference Store.

## Prerequisites

### Docker

Docker is an open platform for developing, shipping, and running applications. We will run Docker containers for Postgres, Redis, and [all of the Featureform services](../system-architecture.md) on Minikube. It can be [downloaded here](https://docs.docker.com/get-docker/).

### Minikube

minikube is a local Kubernetes deployment made for testing and exploring Kubernetes. It is not made for production, but mimics most of the main functionality of a production Kubernetes cluster. [Follow their tutorial](https://minikube.sigs.k8s.io/docs/start/) to deploy and configure a local Minikube cluster.

### Helm

Helm is a package manager for Kubernetes. We can use it to deploy Featureform, Postgres, and Redis on our minikube deployment. But first, we have to [install it using their guide](https://helm.sh/docs/intro/quickstart/).

## Install Certificate Manager

Cert-manager allows us to create self-signed and public TLS certificates for connecting the Python client to Featureform.

```
helm repo add jetstack https://charts.jetstack.io
```

```
helm repo update
```

```
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.8.0 \
    --namespace cert-manager \
    --create-namespace
```

## Deploy Featureform

Deploying Featureform is simple using Helm.

```
helm repo add featureform https://storage.googleapis.com/featureform-helm/
```

```
helm repo update
```

```
helm install my-featureform featureform/featureform --set global.publicCert=false
```

This is starts a vanilla Featureform deployment, but it can be further configured using a set of Helm variables.

## Save Certificate

Our Featureform cluster generated a self-signed TLS certificate that can be used for testing locally.&#x20;

To save it, we run:

```
kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt'| base64 -d > tls.crt
```

We will use this later when connecting with the Python client.

## Enable Minikube Ingress

To access our cluster, we need to enable minikube's ingress and network tunnel.&#x20;

```
minikube addons enable ingress
```

```
minikube tunnel
```

## Deploy The Quickstart Demo

We provide an additional helm chart that contains Redis and Postgres, as well as a loading job that fills Postgres with demo data.&#x20;

```
helm install quickstart featureform/quickstart
```

## Configure Featureform with Providers

Now that all of our infrastructure is deployed, we can add Postgres and Redis as providers for Featureform using the Python API and Featureform CLI, which can be seen in our [Quickstart guide](../quickstart-kubernetes.md#step-5-register-providers).

Since we're using a self-signed certificate, we can run the CLI using the certificate flag instead.

```
export FEATUREFORM_HOST="localhost:443"
featureform apply <file> --cert tls.cert
```
