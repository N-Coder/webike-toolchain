from codecs import open
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='webike',
    version='0.0.1',
    description='WeBike Data Processing Toolchain',
    long_description=long_description,
    url='https://github.com/N-Coder/webike-toolchain',
    author='Simon Dominik `Niko` Fink',
    author_email='sfink@uwaterloo.ca',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    install_requires=[
        'matplotlib>=2.0.0b4',
        'metar>=1.4.0',
        'numpy>=1.11.0',
        'pygobject>=3.20.1',
        'PyMySQL>=0.7.9',
        'scipy>=0.18.0',
        'tabulate>=0.7.5',
        'wget>=3.2'
    ],
    include_package_data=True,
    package_data={
        'webike': [
            'ui/glade/*'
        ],
    },
    entry_points={
        "console_scripts": [
            "webike-timeline = webike.ui.UI:main",
            "webike-histogram = webike.Histogram:main",
            "webike-prepocess = webike.Preprocess:main",
        ]
    },
)
