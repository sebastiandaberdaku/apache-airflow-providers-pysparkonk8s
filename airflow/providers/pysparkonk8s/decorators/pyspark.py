from typing import Mapping, Sequence, Callable, Any

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.pysparkonk8s.operators import PySparkOnK8sOperator
from airflow.providers.pysparkonk8s.utils import ensure_spark_is_kwarg


class _PySparkOnK8sDecoratedOperator(DecoratedOperator, PySparkOnK8sOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when calling your callable (templated)
    """
    custom_operator_name = "@task.pyspark_on_k8s"

    # Airflow considers the field names present in template_fields for Jinja templating while rendering the operator.
    template_fields: Sequence[str] = tuple({
        *DecoratedOperator.template_fields,
        *PySparkOnK8sOperator.template_fields,
    })
    # The parameter can also contain a file name, for example, a bash script or a SQL file. You need to add the
    # extension of your file in template_ext. If a template_field contains a string ending with the extension mentioned
    # in template_ext, Jinja reads the content of the file and replace the templates with actual value. Note that Jinja
    # substitutes the operator attributes and not the args.
    template_ext: Sequence[str] = tuple({
        *DecoratedOperator.template_ext,
        *PySparkOnK8sOperator.template_ext,
    })
    # The template_fields_renderers dictionary defines in what style the values from template_fields renders in Web UI.
    template_fields_renderers: Mapping[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **PySparkOnK8sOperator.template_fields_renderers,
    }
    # Each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = tuple({
        *DecoratedOperator.shallow_copy_attrs,
        *PySparkOnK8sOperator.shallow_copy_attrs,
    })

    def __init__(
            self,
            *,
            python_callable: Callable,
            op_args: Any,
            op_kwargs: dict[str, Any],
            **kwargs: Any,
    ) -> None:
        python_callable = ensure_spark_is_kwarg(python_callable)
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def pyspark_on_k8s_task(
        python_callable: Callable | None = None,
        multiple_outputs: bool | None = None,
        **kwargs: Any,
) -> TaskDecorator:
    """
    PySparkOnK8sOperator decorator.

    This wraps a function to be executed in K8s using PySparkOnK8sOperator.

    @see: https://airflow.apache.org/docs/apache-airflow/stable/howto/create-custom-decorator.html

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be unrolled to multiple XCom values.
        Dict will unroll to xcom values with keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PySparkOnK8sDecoratedOperator,
        **kwargs,
    )
