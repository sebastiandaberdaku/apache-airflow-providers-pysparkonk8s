# Setup a local testing environment with minikube

## Requirements
minikube (setup instructions [here](https://minikube.sigs.k8s.io/docs/start/)) and kubectl (setup instructions [here](https://kubernetes.io/docs/tasks/tools/)).

## Starting the minikube cluster

From a terminal with administrator access (but not logged in as root), run:
```shell
minikube start
```

First, let's create the `airflow` namespace where we will install our Apache Airflow cluster.
```shell
kubectl create namespace airflow
```

Mount the `examples/dags` inside the cluster. Navigate inside the `examples/` folder and run:
```shell
kubectl apply -f dags-volume.yaml
```
This will setup a PersistentVolume of type `hostPath` for the `/mnt/airflow/dags` node path, and PersistentVolumeClaim
`dags` that will be mounted as a persistent volume inside the Airflow Pods.

In a separate, dedicated terminal run the following command:
```shell
minikube mount ./dags/:/mnt/airflow/dags
```

Install Airflow using an image with our provider pre-installed.
```shell
helm install airflow apache-airflow/airflow \
  --version 1.11.0 \
  --timeout 30m \
  --namespace airflow \
  --create-namespace \
  --set triggerer.enabled=false \
  --set statsd.enabled=false \
  --set images.airflow.repository=sebastiandaberdaku/airflow \
  --set images.airflow.tag=2.8.1-python3.10-java17-pyspark3.5.0 \
  --set images.airflow.pullPolicy=Always \
  --set executor=KubernetesExecutor \
  --set dags.persistence.enabled=true \
  --set dags.persistence.existingClaim=dags
```

Install the `pysparkonk8s-addon` Helm chart.
```shell
helm repo add pysparkonk8s https://sebastiandaberdaku.github.io/apache-airflow-providers-pysparkonk8s
helm upgrade --install pysparkonk8s pysparkonk8s/pysparkonk8s-addon --namespace airflow
```

Finally, stop minikube:
```shell
minikube stop
```