import functools
from typing import Any
from typing import Callable
from typing import Optional


def udf(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    max_batch_rows: int = 500,
    num_processes: int = 1,
) -> Callable[..., Any]:
    """
    Apply attributes to a UDF.

    Parameters
    ----------
    func : callable, optional
        The UDF to apply parameters to
    name : str, optional
        The name to use for the UDF in the database
    max_match_rows : int, optional
        The number of rows to batch in the server before sending
        them to the UDF application
    num_processes : int, optional
        The number of sub-processes to spin up to process sub-batches
        of rows of data. This may be used if the UDF is CPU-intensive
        and there are free CPUs in the server the web application
        is running in. If the UDF is very short-running, setting this
        parameter to greater than one will likey cause the UDF to
        run slower since the overhead of the extra processes and moving
        data would be the limiting factor.

    Returns
    -------
    Callable

    """

    assert max_batch_rows >= 1
    assert num_processes >= 1

    def wrapper(*args: Any, **kwargs: Any) -> Callable[..., Any]:
        return func(*args, **kwargs)  # type: ignore

    wrapper._udf_attrs = {  # type: ignore
        k: v for k, v in dict(
            name=name,
            max_batch_rows=max_batch_rows,
            num_processes=num_processes,
        ).items() if v is not None
    }

    if func is None:
        def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
            return functools.update_wrapper(wrapper, func)
        return decorate

    return functools.update_wrapper(wrapper, func)
