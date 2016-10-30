import abc

from webike.util.Utils import zip_prev


class ActivityDetection:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_start(self, sample, previous):
        return False

    @abc.abstractmethod
    def is_end(self, sample, previous):
        return False

    def check_reject_reason(self, cycle):
        return None

    def accumulate_samples(self, new_sample, accumulator):
        return None

    def __call__(self, cycle_samples):
        cycles = []
        discarded_cycles = []
        cycle_start = None
        cycle_acc = None

        for previous, sample in zip_prev(cycle_samples):
            # did cycle start?
            if not cycle_start:
                if self.is_start(sample, previous):
                    # yes, mark this as the beginning
                    cycle_start = sample
                    cycle_acc = self.accumulate_samples(sample, None)

            # did cycle stop?
            else:
                if not self.is_start(sample, previous):
                    # nope, continue counting
                    cycle_acc = self.accumulate_samples(sample, cycle_acc)
                else:
                    # yes, mark this as the end
                    cycle_end = previous
                    cycle = (cycle_start, cycle_end, cycle_acc)
                    # TODO merge consecutive cycles?
                    # only count as cycle if it matches the criteria
                    reject_reason = self.check_reject_reason(cycle)
                    if not reject_reason:
                        cycles.append(cycle)
                    else:
                        discarded_cycles.append(cycle + (reject_reason,))
                    cycle_start = None
                    cycle_acc = None

        return cycles, discarded_cycles
