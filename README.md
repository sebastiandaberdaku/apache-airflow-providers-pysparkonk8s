# apache-airflow-providers-pysparkonk8s
Task with PySparkOnK8s operator Apache Airflow provider package: [Apache Spark](https://spark.apache.org/).

## Provider package
This is a Python package for the ``pysparkonk8s`` Apache Airflow provider. All classes for this provider are in the 
``airflow.providers.pysparkonk8s`` python package.

## Installation
The provided package can be installed with the following command:
```shell
pip install git+https://github.com/sebastiandaberdaku/apache-airflow-providers-pysparkonk8s.git@main
```
or (if git is missing):
```shell
pip install https://github.com/sebastiandaberdaku/apache-airflow-providers-pysparkonk8s/archive/refs/heads/main.tar.gz
```

## pysparkonk8s-addon
The `chart/` folder contains the `pysparkonk8s-addon` Helm Chart that installs the required role and role-binding 
for the provider.

### Installing `pysparkonk8s-addon`
To install the addon use the following command:
```shell
helm repo add pysparkonk8s https://sebastiandaberdaku.github.io/apache-airflow-providers-pysparkonk8s
helm upgrade --install pysparkonk8s pysparkonk8s/pysparkonk8s-addon --namespace airflow
```

## Testing
### Debug interactively with dag.test()
Airflow 2.5.0 introduced the `dag.test()` method which allows you to run all tasks in a DAG within a single serialized 
Python process without running the Airflow scheduler. The `dag.test()` method allows for faster iteration and use of IDE 
debugging tools when developing DAGs. 

### Prerequisites
Ensure that your testing environment has:
* Airflow 2.5.0 or later. You can check your version by running `airflow version`.
* All provider packages that your DAG uses.
* An initialized Airflow metadata database, if your DAG uses elements of the metadata database like XCom. The Airflow 
metadata database is created when Airflow is first run in an environment. You can check that it exists with `airflow db 
check` and initialize a new database with `airflow db migrate` (`airflow db init` in Airflow versions pre-2.7).

### Installing required dependencies
The project dependencies are provided in the `requirements-dev.txt` file. We recommend creating a dedicated Conda 
environment for managing the dependencies.

This project was tested with Python version 3.10 and Apache Airflow version 2.8.1. We set the following environment 
variables that will be used throughout the environment setup.

```shell
export PYTHON_VERSION="3.10"
export AIRFLOW_VERSION="2.8.1"
```

Setup and activate Conda environment with the following command:
```shell
conda create --name pysparkonk8s python=${PYTHON_VERSION} -y
conda activate pysparkonk8s
```

Install the dependencies with the following command.
```shell
pip install -r requirements-dev.txt -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

### Set up a Database Backend
Airflow requires a Database Backend to manage its metadata. In production environments, you should consider setting up a 
database backend to PostgreSQL, MySQL, or MSSQL. By default, Airflow uses SQLite, which is intended for development 
purposes only.

For our tests we will use SQLite. To initialize the SQLite database please execute the following commands:
```shell
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db
airflow db migrate
```
This will create the `airflow.db` file in your current directory. This file is already included in `.gitignore`, however 
please make sure you are not accidentally adding it to git if you change the default file path.

If you have any issues in setting up the SQLite database please refer to the 
[official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-sqlite-database).

### Running the tests
The tests can be run with the following commands.

```shell
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db
export AIRFLOW_HOME=airflow_home
export AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=False
pytest .
```