from collections import Counter

import matplotlib.pyplot as plt
import numpy as np

IMEIS = ['0587', '0603', '0636', '0657', '0665', '0669', '1210', '1473', '2910', '3014', '3215', '3410', '3469', '4381',
         '5233', '5432', '6089', '6097', '6473', '6904', '6994', '7303', '7459', '7517', '7710', '8508', '8664', '8870',
         '9050', '9407', '9519']


def plot_weather(weather, filename):
    for key, value in weather.items():
        plt.clf()
        if key == 'weather':
            counter = Counter(value)
            frequencies = counter.values()
            names = counter.keys()
            x_coordinates = np.arange(len(counter))
            plt.bar(x_coordinates, frequencies, align='center')
            plt.xticks(x_coordinates, names)
        elif key.endswith('_flag') or key == 'datetime' or key == 'quality':
            continue
        else:
            plt.hist(value, bins=25)
        plt.title('Weather - ' + key)
        plt.savefig(filename.format(key))
