# Kubernetes

This guide will walk through deploying Featureform on Kubernetes. The Featureform ingress currently supports AWS load balancers.&#x20;

## Prerequisites

* An existing Kubernetes Cluster in AWS
* A domain name that can be directed at the Featureform load balancer

## Step 1: Add Helm repos

Add Certificate Manager and Featureform Helm Repos.&#x20;

```
helm repo add featureform https://storage.googleapis.com/featureform-helm/ 
helm repo add jetstack https://charts.jetstack.io 
helm repo update
```

## Step 2: Install Helm Charts

### Certificate Manager&#x20;

If Certificate Manager has not yet been installed, install it before installing Featureform.

```
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.8.0 \
    --namespace cert-manager \
    --create-namespace
```

### Featureform

Install Featureform with the desired domain name. Featureform will automatically provision the public TLS certificate when the specific domain name is routed to the Featureform loadbalancer.

```
helm install <release-name> featureform/featureform \
    --set global.hostname=<your-domain-name>
    --set global.publicCert=true
```

## Step 3: Domain routing

After Featureform has created its load balancer, you can create a CNAME record for your domain that points to the Featureform load balancer.&#x20;

Public TLS certificates will be generated automatically once the record has been created.
