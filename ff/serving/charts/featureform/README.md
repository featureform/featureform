##To Create a Sandbox

###Prerequisites

- kubectl
- aws cli
- eksctl
- helm

###Create Cluster
Run:
````
eksctl create cluster \
--name <CLUSTER_NAME> \
--version 1.21 \
--region us-east-1 \
--nodegroup-name linux-nodes \
--nodes 1 \
--nodes-min 1 \
--nodes-max 5 \
--with-oidc \
--managed
````

###Install Certificate Manager

`kubectl create namespace cert-manager`

`helm repo add jetstack https://charts.jetstack.io`

`helm repo update`
```
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.0.4 \
    --namespace cert-manager
```

###Install Sandbox
Go to ff/serving/charts and run:

`helm install <NAME> ./featureform/` 

(This will create a sandbox accessible at <NAME>-sandbox.featureform.com)

Wait until pods are ready by checking:

`kubectl get pods`

(Currently, metadata and coordinator will just terminate and restart repeatedly until etcd is ready)

###Forward DNS
1. Run:
`kubectl get ingress`
2. Copy the hostname

3. Go to:
https://us-east-1.console.aws.amazon.com/route53/v2/hostedzones?region=us-east-1#ListRecordSets/Z09940351UD84OM1DEZJB

4. Click "Create record"
5. Create a record with the copied hostname
6. Change "Record type" to "CNAME"
7. Copy the address from `kubectl get ingress`
8. Paste the address in the "Value" field
9. Hit "Create records"

