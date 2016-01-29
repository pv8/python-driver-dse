# Copyright 2016 DataStax, Inc.

from __future__ import print_function
from dse import __version__

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
from distutils.cmd import Command

long_description = ""
with open("README.rst") as f:
    long_description = f.read()


class DocCommand(Command):

    description = "generate documentation"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        path = "doc"
        mode = "html"

        import os
        import shutil
        shutil.rmtree(path, ignore_errors=True)  # is this required?
        os.makedirs(path)

        import subprocess
        try:
            output = subprocess.check_output(
                ["sphinx-build", "-b", mode, "docs", path],
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError("Documentation step '%s' failed: %s: %s" % (mode, exc, exc.output))
        else:
            print(output)

        print("")
        print("Documentation step '%s' performed, results here:" % mode)
        print("   file://%s/%s/index.html" % (os.path.dirname(os.path.realpath(__file__)), path))

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
    ],
    cmdclass={'doc': DocCommand})
