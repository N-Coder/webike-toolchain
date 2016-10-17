import collections
import logging
from datetime import timedelta

from webike.util.Utils import zip_prev

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


def can_merge(last_cycle, new_cycle, merge_within):
    # if last_cycle is actually a list, use the last value
    if isinstance(last_cycle, collections.Sequence) and not isinstance(last_cycle, tuple):
        if len(last_cycle) < 1: return False
        last_cycle = last_cycle[-1]

    last_start = last_cycle[0]['Stamp']
    last_end = last_cycle[1]['Stamp']
    new_start = new_cycle[0]['Stamp']
    new_end = new_cycle[1]['Stamp']

    # if both ranges intersect, they can be merged
    if max(last_start, new_start) <= min(last_end, new_end): return True

    gap = new_start - last_end
    assert gap > timedelta(seconds=0), "new_cycle ({}) must start after last_cycle ({})".format(last_cycle, new_cycle)
    # only merge if the time gap between the two cycles is less than merge_within
    if gap > merge_within: return False
    # don't merge small samples with a big gap between them
    if new_end - new_start < gap: return False
    if last_end - last_start < gap: return False
    return True


def merge_cycles(cycles, merge_within=timedelta(minutes=30)):
    cycles = sorted(cycles, lambda a, b: (a[0]['Stamp'] - b[0]['Stamp']) / timedelta(seconds=1))
    second = None
    merged = False
    for first, second in zip_prev(cycles):
        if not first or merged:
            merged = False
            continue

        if can_merge(first, second, merge_within):
            start = first[0]  # start = first.start
            end = second[1]  # end = second.end
            sample_count = first[2] + second[2]  # samples_count ~= sum(samples_counts)
            thresh_value = second[0]['Stamp'] - first[1]['Stamp']  # thresh_value = time gap
            merged = True
            yield (start, end, sample_count, thresh_value, 'M')  # type = merged
        else:
            yield first

    if not merged: yield second


def extract_cycles_soc(charge_samples,
                       charge_thresh_start=0.001, charge_thresh_end=0.001, min_charge_samples=100,
                       max_sample_delay=timedelta(minutes=10), min_charge_time=timedelta(minutes=30),
                       min_charge_amount=0.05, merge_within=timedelta(minutes=30)):
    """Detect charging cycles based on an increasing state of charge."""

    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None
    charge_sample_count = 0

    for last_sample, sample in zip_prev(charge_samples):
        # did charging start?
        if not charge_start:
            if sample['soc_diff_smooth'] > charge_thresh_start:
                # yes, because SoC is increasing
                charge_start = sample
                charge_sample_count = 1

        # did charging stop?
        else:
            if sample['soc_diff_smooth'] < charge_thresh_end:
                # yes, because SoC isn't increasing anymore
                charge_end = sample
            elif sample['Stamp'] - last_sample['Stamp'] > max_sample_delay:
                # yes, because we didn't get a sample for the last few mins
                charge_end = last_sample
            else:
                # nope, continue counting
                charge_sample_count += 1

            if charge_end:
                if can_merge(cycles, (charge_start, charge_end), merge_within):
                    # merge with previous cycle if they are close together
                    charge_amount = charge_end['soc_smooth'] - cycles[-1][0]['soc_smooth']
                    cycles[-1] = (cycles[-1][0], charge_end, cycles[-1][3] + charge_sample_count, charge_amount)
                else:
                    if can_merge(discarded_cycles, (charge_start, charge_end), merge_within):
                        # merge with previous discarded cycle if they are close together
                        # and check again whether they should be added altogether
                        charge_start = discarded_cycles[-1][0]
                        charge_sample_count += discarded_cycles[-1][2]
                        del discarded_cycles[-1]

                    if charge_end['Stamp'] - charge_start['Stamp'] > min_charge_time \
                            and charge_sample_count > min_charge_samples \
                            and charge_end['soc_smooth'] - charge_start['soc_smooth'] > min_charge_amount:
                        # only count as charging cycle if it lasts for more than a few mins, we got enough samples
                        # and actually increased the SoC
                        cycles.append((charge_start, charge_end, charge_sample_count,
                                       charge_end['soc_smooth'] - charge_start['soc_smooth']))
                    else:
                        discarded_cycles.append((charge_start, charge_end, charge_sample_count,
                                                 charge_end['soc_smooth'] - charge_start['soc_smooth']))

                charge_start = None
                charge_end = None
                charge_sample_count = 0
    return cycles, discarded_cycles
