import functools
import datetime

def profiler(func):  # type: ignore
    """
    Returns profiling decorator, which counts calls of function
    and measure last function execution time.
    Results are stored as function attributes: `calls`, `last_time_taken`
    :param func: function to decorate
    :return: decorator, which wraps any function passed
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        fl = False
        if type(wrapper.calls) is int:
            fl = True
            wrapper.calls = 0.0

        start_time = datetime.datetime.now()
        result = func(*args, **kwargs)
        wrapper.calls += 1.0

        if fl:
            wrapper.last_time_taken = (datetime.datetime.now() - start_time).total_seconds()
            wrapper.calls = int(wrapper.calls)
        return result

    wrapper.calls = 0
    return wrapper
