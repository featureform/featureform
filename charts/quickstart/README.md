# Quickstart Helm Chart

This Helm chart will create a Postgres and Redis instance on Kubernetes. It will load the Postgres instance with a 
Transactions table containing 10k rows of data from https://featureform-demo-files.s3.amazonaws.com/transactions.csv


## Requirements
- Kubernetes >=v1.19

## Global parameters
| Name                     | Description                                                                        | Default |
|--------------------------|------------------------------------------------------------------------------------|:-------:|
| global.data_size         | The amount of data to load. "short" will load 10k rows. "long"  will load 1M rows. | "short" |

