import logging
import sys
from collections import Counter

import matplotlib.pyplot as plt
import numpy as np

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


def plot_weather(hist_datasets, out_file=None, fig_offset=None):
    logger.info("Plotting weather graphs")
    # get all keys in all datasets
    keys = {key for ds in hist_datasets.values() for key in ds.keys()}

    for key in keys:
        if key.endswith('_flag') or key == 'datetime' or key == 'quality':
            continue
        if fig_offset is not None:
            plt.figure(fig_offset)
            fig_offset += 1
        else:
            plt.clf()

        if key.startswith('weather'):
            counters = []
            labels = set()
            for name, ds in hist_datasets.items():
                if key in ds:
                    ds[key] = [w if w else "" for w in ds[key]]
                    counter = Counter([w for v in ds[key] for w in v.split(",")])
                    del counter['NA']
                    counters.append((name, counter))
                    for label in counter.keys():
                        labels.add(label)

            x_coordinates = np.arange(len(labels))
            plt.xticks(x_coordinates, labels)
            prop_iter = iter(plt.rcParams['axes.prop_cycle'])
            bars = []
            for (name, counter) in counters:
                integral = sum(counter.values())
                freq = [counter[label] / integral * 100 for label in labels]
                bar = plt.bar(x_coordinates, freq, label=name + '-' + key, align='center',
                              facecolor=next(prop_iter)['color'])
                bars.append(bar)
            order_bars(bars)
        else:
            value_lists = [(name, ds[key]) for name, ds in hist_datasets.items() if key in ds]
            min_val = min([min(l, default=-sys.maxsize) for n, l in value_lists])
            max_val = max([max(l, default=sys.maxsize) for n, l in value_lists])
            bins = np.linspace(min_val, max_val, 25)

            hists = []
            for name, vl in value_lists:
                hist = plt.hist(vl, bins=bins, label=name + " - " + key, normed=True)
                hists.append(hist)
            order_hists(hists)

        plt.title("Weather - " + key)
        plt.legend()
        if out_file is not None:
            plt.savefig(out_file.format(key))

    logger.info("Graphs finished")
    return fig_offset


def order_hists(hists):
    """see http://stackoverflow.com/a/8764575/805569"""
    all_ns = [hist[0] for hist in hists]
    all_patches = [hist[2] for hist in hists]

    z_orders = -np.argsort(all_ns, axis=0)

    for zrow, patchrow in zip(z_orders, all_patches):
        assert len(zrow) == len(patchrow)
        for z_val, patch in zip(zrow, patchrow):
            patch.set_zorder(z_val)


def order_bars(barsets):
    all_ns = [[bar._height for bar in bars] for bars in barsets]
    all_patches = [bars.patches for bars in barsets]

    z_orders = -np.argsort(all_ns, axis=0)

    for zrow, patchrow in zip(z_orders, all_patches):
        assert len(zrow) == len(patchrow)
        for z_val, patch in zip(zrow, patchrow):
            patch.set_zorder(z_val)
