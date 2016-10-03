import logging
from datetime import datetime, timedelta
from time import perf_counter

from util.Logging import BraceMessage as __


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

    if not (sample and label in sample and sample[label]) or \
            (callable(is_valid) and not is_valid(sample, last_sample, label)):
        if callable(default_value):
            sample[label_smooth] = default_value(sample, last_sample, label)
        else:
            sample[label_smooth] = default_value
    else:
        if not (last_sample and label_smooth in last_sample and last_sample[label_smooth]):
            # 1nd sensible value in the list, use it as starting point for the smoothing
            sample[label_smooth] = sample[label]
        else:
            # current and previous value available, apply the smoothing function
            sample[label_smooth] = alpha * last_sample[label_smooth] \
                                   + (1 - alpha) * sample[label]
    return sample


def smooth_ignore_missing(sample, last_sample, label):
    return last_sample[label] if last_sample else None


def progress(iterable, logger=logging, level=logging.INFO, delay=5,
             msg="Processed {countf} entries after {timef}s ({ratef} entries per second)"):
    msg = msg.format(countf='{count:,}', timef='{time:.2f}', ratef='{rate:,.2f}')

    last_print = start = perf_counter()
    last_rows = 0
    for nr, val in enumerate(iterable):
        if (perf_counter() - last_print) > delay:
            logger.log(level, __(msg, count=nr, time=perf_counter() - start,
                                 rate=(nr - last_rows) / (perf_counter() - last_print)))
            last_print = perf_counter()
            last_rows = nr
        yield val
