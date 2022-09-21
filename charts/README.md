# Helm Charts

Helm charts for the Featureform Kubernetes cluster.

##Directories
- *featureform*: The main Helm chart. This contains all the services required for 
Featureform in this repository, as well as dependencies for ETCD and Nginx Ingress.
- *quickstart*: The Quickstart Helm chart for Featureform. It contains an instance of
Postgres and Redis, with a loader job that loads Postgres with example dataset within
the Transactions table. 