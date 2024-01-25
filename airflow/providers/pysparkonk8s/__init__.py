import packaging.version
from airflow.providers.pysparkonk8s.get_provider_info import __name__, __version__

__all__ = ["__version__", "get_provider_info"]

try:
    from airflow import __version__ as airflow_version
except ImportError:
    from airflow.version import version as airflow_version

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse("2.6.0"):
    raise RuntimeError(f"The package `{__name__}:{__version__}` needs Apache Airflow 2.6.0+")
