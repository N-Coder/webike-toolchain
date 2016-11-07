import abc
import collections
import warnings
from datetime import timedelta
from typing import List

from iss4e.util import zip_prev

from webike.util.constants import TD0

Cycle = collections.namedtuple('Cycle', ['start', 'end', 'stats', 'reject_reason'])


class ActivityDetection(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_start(self, sample, previous):
        return False

    @abc.abstractmethod
    def is_end(self, sample, previous):
        return False

    def check_reject_reason(self, cycle: Cycle):
        return None

    def accumulate_samples(self, sample, accumulator):
        return None

    def __call__(self, cycle_samples) -> (List[Cycle], List[Cycle]):
        self.cycles = []
        self.discarded_cycles = []
        self.cycle_start = None
        self.cycle_acc = None
        for previous, sample in zip_prev(cycle_samples):
            # did cycle start?
            if not self.cycle_start:
                if self.is_start(sample, previous):
                    # yes, mark this as the beginning
                    self.cycle_start = sample
                    self.cycle_acc = self.accumulate_samples(sample, {})

            # did cycle stop?
            else:
                if not self.is_end(sample, previous):
                    # nope, continue counting
                    self.cycle_acc = self.accumulate_samples(sample, self.cycle_acc)
                else:
                    # yes, mark this as the end
                    self.store_cycle(Cycle(
                        start=self.cycle_start, end=previous,
                        stats=self.cycle_acc, reject_reason=None))
                    self.cycle_start = None
                    self.cycle_acc = None

        return self.cycles, self.discarded_cycles

    def store_cycle(self, cycle: Cycle):
        # only count as cycle if it matches the criteria
        reject_reason = self.check_reject_reason(cycle)
        if not reject_reason:
            self.cycles.append(cycle)
        else:
            self.discarded_cycles.append(cycle._replace(reject_reason=reject_reason))


class MergeMixin(object):
    def store_cycle(self, cycle: Cycle):
        # try to merge with as much previous cycles as possible
        while True:
            if self.can_merge(self.cycles, cycle):
                merge_with = self.cycles[-1]
                del self.cycles[-1]
            elif self.can_merge(self.discarded_cycles, cycle):
                merge_with = self.discarded_cycles[-1]
                del self.discarded_cycles[-1]
            else:
                break

            cycle = cycle._replace(start=merge_with.start, stats=self.merge_stats(merge_with.stats, cycle.stats))
        # and then store again
        super().store_cycle(cycle)

    def can_merge(self, last_cycle, new_cycle: Cycle):
        # if last_cycle is actually a list, use the last value
        if isinstance(last_cycle, collections.Sequence) and not isinstance(last_cycle, tuple):
            if len(last_cycle) < 1: return False
            last_cycle = last_cycle[-1]

        assert self.extract_cycle_time.__func__ != getattr(MergeMixin, 'extract_cycle_time'), \
            "you must override MergingActivityDetection.extract_cycle_time " \
            "when using the default can_merge implementation"

        last_start, last_end = self.extract_cycle_time(last_cycle)
        new_start, new_end = self.extract_cycle_time(new_cycle)

        # if both ranges intersect, they can be merged
        if max(last_start, new_start) <= min(last_end, new_end):
            warnings.warn("merging intersecting cycles will break stats\n{} + {}".format(last_cycle, new_cycle))
            return True

        gap = new_start - last_end
        assert (isinstance(gap, timedelta) and gap > TD0) or (not isinstance(gap, timedelta) and gap > 0), \
            "new_cycle ({}) must start after last_cycle ({})".format(last_cycle, new_cycle)

        return self.can_merge_times(last_start, last_end, new_start, new_end)

    def extract_cycle_time(self, cycle: Cycle):
        return 0, 1

    def can_merge_times(self, last_start, last_end, new_start, new_end):
        return False

    def merge_stats(self, stats1, stats2):
        return stats1
