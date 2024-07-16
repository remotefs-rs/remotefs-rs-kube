#!/bin/bash

MINIKUBE=$(which minikube)
KUBECTL="$MINIKUBE kubectl"
if [ -z "$MINIKUBE" ]; then
  echo "Minikube is not installed. Please install it first."
  echo '$ curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64'
  echo '$ sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64'

  exit 1
fi

# start minikube
minikube start

export MINIKUBE_IP="$(minikube ip)"
echo "minikube IP: $MINIKUBE_IP"

$KUBECTL create namespace default

cargo test --features integration-tests $@
RC=$?

# delete all
kubectl delete pods --all

# stop
minikube stop

exit $RC
