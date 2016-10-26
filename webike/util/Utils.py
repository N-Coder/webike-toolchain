import inspect
import logging
from datetime import datetime, timedelta
from time import perf_counter

from webike.util.Logging import BraceMessage as __


def daterange(start, stop=datetime.now(), step=timedelta(days=1)):
    """Similar to :py:func:`builtins.range`, but for dates"""
    if start < stop:
        cmp = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmp = lambda a, b: a > b
        inc = lambda a: a - step
    yield start
    start = inc(start)
    while cmp(start, stop):
        yield start
        start = inc(start)


def zip_prev(iterable):
    """Return the sequence as tuples together with their predecessors"""
    last_val = None
    for val in iterable:
        yield (last_val, val)
        last_val = val


def discharge_curr_to_ampere(val):
    """Convert DischargeCurr from the DB from the raw sensor value to amperes"""
    return (val - 504) * 0.033 if val else 0


def differentiate(samples, label, label_diff=None, delta_time=timedelta(seconds=1)):
    if not label_diff:
        label_diff = label + '_diff'

    last_sample = None
    for sample in samples:
        if last_sample is None or last_sample[label] is None or sample[label] is None:
            sample[label_diff] = 0
        else:
            sample[label_diff] = (sample[label] - last_sample[label]) / \
                                 ((sample['Stamp'] - last_sample['Stamp']) / delta_time)
        yield sample
        last_sample = sample


def smooth(samples, label, label_smooth=None, alpha=.95, default_value=None, is_valid=None):
    """Smooth values using the formula
    `samples[n][label_smooth] = alpha * samples[n-1][label_smooth] + (1 - alpha) * samples[n][label]`

    If a value isn't available, or `is_valid(sample, last_sample, label)` returns false,
     the previous smoothed value is used.
    If none of these exist, default_value is used. If default_value is callable,
     `default_value(sample, last_sample, label)` will be called
    """
    if not label_smooth:
        label_smooth = label + '_smooth'

    last_sample = None
    for sample in samples:
        yield smooth1(sample, last_sample, label, label_smooth, alpha, default_value, is_valid)
        last_sample = sample


def smooth1(sample, last_sample, label, label_smooth=None, alpha=.95, default_value=None, is_valid=None):
    if not label_smooth:
        label_smooth = label + '_smooth'

    if not (sample and label in sample and sample[label] is not None) or \
            (callable(is_valid) and not is_valid(sample, last_sample, label)):
        if callable(default_value):
            sample[label_smooth] = default_value(sample, last_sample, label, label_smooth)
        else:
            sample[label_smooth] = default_value
    else:
        if not (last_sample and label_smooth in last_sample and last_sample[label_smooth] is not None):
            # 1nd sensible value in the list, use it as starting point for the smoothing
            sample[label_smooth] = sample[label]
        else:
            # current and previous value available, apply the smoothing function
            sample[label_smooth] = alpha * last_sample[label_smooth] \
                                   + (1 - alpha) * sample[label]
    return sample


def smooth_ignore_missing(sample, last_sample, label, label_smooth):
    if last_sample:
        if label_smooth in last_sample:
            return last_sample[label_smooth]
        elif label in last_sample:
            return last_sample[label]

    return None


def smooth_reset_stale(max_sample_delay):
    return lambda sample, last_sample, label: last_sample and last_sample['Stamp'] - sample['Stamp'] < max_sample_delay


def progress(iterable, logger=logging, level=logging.INFO, delay=5,
             msg="{verb} {countf} {name} after {timef}s ({ratef}/{avgratef} {name} per second)",
             objects="entries", verb="Processed"):
    msg = msg.format(countf='{count:,}', timef='{time:.2f}', ratef='{rate:,.2f}', avgratef='{avgrate:,.2f}',
                     name=objects, verb=verb)

    last_print = start = perf_counter()
    last_rows = nr = 0
    val = None

    def print(last):
        nonlocal last_rows, last_print
        if (perf_counter() - last) > delay:
            logger.log(level, __(msg, count=nr, time=perf_counter() - start, value=val,
                                 rate=(nr - last_rows) / (perf_counter() - last_print),
                                 avgrate=nr / (perf_counter() - start)))
            last_print = perf_counter()
            last_rows = nr

    for nr, val in enumerate(iterable):
        print(last_print)
        yield val
        # print(last_print)

    print(start)


def dump_args(frame):
    """
    Print this information using
        "{}(\n  {})".format(func_name, ",\n  ".join(["{} = {}".format(k, v) for k, v in arg_list])
    """
    arg_names, varargs, kwargs, values = inspect.getargvalues(frame)
    arg_list = list([(n, values[n]) for n in arg_names])
    if varargs:
        arg_list.append(("*args", varargs))
    if kwargs:
        arg_list.extend([("**" + k, v) for k, v in kwargs.items()])
    func_name = inspect.getframeinfo(frame)[2]
    return func_name, arg_list
