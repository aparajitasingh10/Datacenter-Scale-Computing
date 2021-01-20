##
## You provide this to build and push your docker images
##

docker build -t gcr.io/datacenter-293019/rest -f /home/apsi2875/lab-7-facerec-kube-aparajitasingh10/rest/Dockerfile-rest .
docker build -t gcr.io/datacenter-293019/logs -f /home/apsi2875/lab-7-facerec-kube-aparajitasingh10/rest/Dockerfile-logs .


docker push gcr.io/datacenter-293019/rest:latest

docker push gcr.io/datacenter-293019/logs:latest

kubectl apply -f rest-deployment.yaml
kubectl apply -f rest-service.yaml
kubectl apply -f rest-ingress.yaml
kubectl apply -f logs-deployment.yaml