# Deployment Overview

Featureform has multiple deployment options to suit different use cases. For more information and specific
capabilities, see the following [Deployment Comparison](compatibility.md) page.

## [Docker](https://docs.featureform.com/quickstart-docker)
Docker is a flexible deployment option. It supports most features included in the Kubernetes deployment, but
does not have built in scaling. It can be run locally or hosted in a cloud. It is designed as an evaluation tool and 
for small scale deployments until a full Kubernetes deployment is required.

## [Kubernetes](https://docs.featureform.com/minikube) 
Kubernetes is the most fully featured deployment option. It supports all features and is designed to scale and distribute
workloads natively. It is suggested when a production deployment is required and Kubernetes can be supported. 