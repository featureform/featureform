##Deployment Steps

###Prerequisites

- kubectl
- aws cli
- terraform
- helm

###Create Cluster
In the terraform/ directory, run:
````
terraform init
terraform apply
````
###Setup kubeconfig
In the terraform/ directory, run:

``aws eks --region $(terraform output -raw region) update-kubeconfig --name $(terraform output -raw cluster_name)``


###Install Certificate Manager
`helm repo add jetstack https://charts.jetstack.io`

`helm repo update`
```
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.0.4 \
    --namespace cert-manager \
    --create-namespace
```

###Install featureform
Go to ff/serving/charts and run:

`helm install <NAME> ./featureform/ --set global.hostname=<DOMAIN_NAME>` 

Where <DOMAIN_NAME> is the desired domain name that you own
and <NAME> is your choice of name for the helm release

###Create DNS Record
Run:
``kubectl get ingress``

and select the url in the "ADDRESS" field. Something like:
xxxxxxxxxxxxxxxxxx-xxxxxxxxxx.us-east-1.elb.amazonaws.com

Then create a cname record directing your <DOMAIN_NAME> to the ADDRESS in your domain provider

Featureform with automatically create a public TLS certificate for your hostname. 
The status can be checked with
``kubectl get certificate``

###Usage

Wait until pods are ready by checking:

`kubectl get pods`

When everything is in a "Running" state, go to your <DOMAIN_NAME> to view the Featureform dashboard
