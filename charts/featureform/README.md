# Featureform Helm Chart

This Helm chart will initialize Featureform on Kubernetes. Compatible on AWS EKS, 
GCP GKE, Azure AKS, Minikube, and Bare Metal. 

This chart installs the Nginx Ingress controller by default.


## Requirements
- Kubernetes v1.19-v1.21
- Certificate Manager >=v1.8.0

## Global parameters
| Name                     | Description                                                                                                                                  |      Default       |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|:------------------:|
| global.hostname          | The hostname where the cluster will be accessible. Required to terminate the TLS certificate for GRPC.                                       |    "localhost"     |
| global.version           | The Docker container tag to pull. The default value is overwritten  with the latest deployment version when pulling from artifacthub         |      "0.0.0"       |
| global.repo              | The Docker repo to pull the images from                                                                                                      |  "featureformcom"  |
| global.pullPolicy        | The container pull policies                                                                                                                  |      "Always"      |
| global.publicCert        | Whether to use a public TLS certificate or a self-signed on. If true, the public certificate is generated for the provided global.hostname.  |      "false"       |
| global.k8s_runner_enable | If true, uses a Kubernetes Job to run Featureform jobs. If false, Featureform jobs are run in the coordinator container in a separate thread |      "false"       |

