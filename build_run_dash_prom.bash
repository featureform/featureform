docker build -f ./dashboard/Dockerfile -t sami1309/featureform-dashboard ./dashboard
docker build -f ./ff/serving/Dockerfile -t sami1309/feature-service ./ff/serving
eval $(minikube -p minikube docker-env)
kubectl delete namespace test-1
kubectl create namespace test-1
kubectl apply -f prometheus -n test-1
kubectl apply -f ff/serving -n test-1
kubectl apply -f dashboard -n test-1
kubectl apply -f ingress.yaml -n test-1