# Copyright 2016 DataStax, Inc.

from __future__ import print_function
from dse import __version__

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup

long_description = ""
with open("README.rst") as f:
    long_description = f.read()


dependencies = ['cassandra-driver > 3.0.0']

setup(
    name='cassandra-driver-dse',
    version=__version__,
    description='DataStax Enterprise extensions for cassandra-driver',
    long_description=long_description,
    packages=['dse'],
    keywords='cassandra,dse,graph',
    include_package_data=True,
    install_requires=dependencies,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ])

