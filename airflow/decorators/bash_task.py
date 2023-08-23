# imports needed for the class definition
from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Callable, Sequence

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.operators.bash import BashOperator
from airflow.utils.decorators import remove_task_decorator

class _BashDecoratedOperator(DecoratedOperator, BashOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    """

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    custom_operator_name: str = "@task.bash"

    def __init__(
        self,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": kwargs["python_callable"],
            "op_args": kwargs["op_args"],
            "op_kwargs": kwargs["op_kwargs"],
        }
        super().__init__(kwargs_to_upstream=kwargs_to_upstream, **kwargs)

    def get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.bash")
        return res

def bash_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wraps a python function into a BashOperator.

    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:BashOperator`

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_BashDecoratedOperator,
        **kwargs,
    ) 
