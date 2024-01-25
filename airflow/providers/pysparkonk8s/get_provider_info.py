from typing import Any

__name__ = "apache-airflow-providers-pysparkonk8s"
__version__ = "1.0.0"


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": __name__,  # Required
        "name": "PySparkOnK8s",  # Required
        "description": "PySpark on Kubernetes Apache Airflow Provider",  # Required
        "operators": [{
            "integration-name": "PySparkOnK8s",
            "python-modules": ["airflow.providers.pysparkonk8s.operators.pyspark"],
        }],
        "task-decorators": [{
            "name": "pyspark_on_k8s",
            "class-name": "airflow.providers.pysparkonk8s.decorators.pyspark.pyspark_on_k8s_task",
        }],
        "versions": [__version__],  # Required
    }
