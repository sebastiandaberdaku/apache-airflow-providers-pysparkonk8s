# Setup a local testing environment with minikube
The current document provides the instructions to setup a local testing environment with minikube where users can test
the available example Apache Airflow DAGs.

## Requirements
* kubectl (setup instructions [here](https://kubernetes.io/docs/tasks/tools/));
* minikube (setup instructions [here](https://minikube.sigs.k8s.io/docs/start/));
* Helm (setup instructions [here](https://github.com/helm/helm#install));
* (Optional) [Kubernetes Dashboard](https://minikube.sigs.k8s.io/docs/handbook/dashboard/), [Open Lens](https://flathub.org/it/apps/dev.k8slens.OpenLens) or [k9s](https://k9scli.io/) or any Kubernetes management tool.

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
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow \
  --version 1.11.0 \
  --wait \
  --timeout 15m \
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
helm install pysparkonk8s pysparkonk8s/pysparkonk8s-addon \
  --version 1.0.0 \
  --namespace airflow \
  --set workerServiceAccount=airflow-worker
```

Install [LocalStack](https://docs.localstack.cloud/overview/) to simulate the AWS cloud services locally.

```shell
helm repo add localstack https://localstack.github.io/helm-charts
helm install localstack localstack/localstack \
  --version 0.6.8 \
  --wait \
  --timeout 15m \
  --namespace localstack \
  --create-namespace 
```

Finally, stop minikube when you are done testing:
```shell
minikube stop
```