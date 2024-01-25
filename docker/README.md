# Image build instructions
To build the image, navigate inside the `docker/` directory and use the following command:
```shell
export AIRFLOW_VERSION="2.8.1"
export PYTHON_VERSION="3.10"
export IMAGE_NAME="sebastiandaberdaku/airflow"
export IMAGE_TAG="${AIRFLOW_VERSION}-python${PYTHON_VERSION}-java17-pyspark3.5.0"
docker build . \
  --tag ${IMAGE_NAME}:${IMAGE_TAG} \
  --network host \
  --build-arg AIRFLOW_VERSION=${AIRFLOW_VERSION} \
  --build-arg PYTHON_VERSION=${PYTHON_VERSION} \
  --no-cache
```

You can push a new image to dockerhub using the CLI:
```shell
docker push ${IMAGE_NAME}:${IMAGE_TAG}
```
