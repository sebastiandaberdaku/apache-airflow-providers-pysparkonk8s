# Building the Helm package
To build the Helm package use the following command from the `docs/` folder of the repository:
```shell
helm package ../chart/ -d .
```
After the package has been built, rebuild the index with:
```shell
helm repo index . --url https://sebastiandaberdaku.github.io/apache-airflow-providers-pysparkonk8s
```
The current repository can be added with the following command:
```shell
helm repo add pysparkonk8s https://sebastiandaberdaku.github.io/apache-airflow-providers-pysparkonk8s
```
The repository can then be updated with the following command:
```shell
helm repo update pysparkonk8s
```
To view the available charts in the repository you can use:
```shell
helm search repo pysparkonk8s
```