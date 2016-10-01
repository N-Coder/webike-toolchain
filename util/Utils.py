from datetime import datetime, timedelta


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
        last_sample = sample
