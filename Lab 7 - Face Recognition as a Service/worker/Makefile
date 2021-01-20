##
## You provide this to build your docker image
##

docker build -t gcr.io/datacenter-293019/worker -f /home/apsi2875/lab-7-facerec-kube-aparajitasingh10/worker/Dockerfile-worker .


docker push gcr.io/datacenter-293019/worker:latest

kubectl apply -f worker-deployment.yaml