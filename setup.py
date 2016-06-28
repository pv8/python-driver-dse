# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from __future__ import print_function
from dse import __version__, _core_driver_target_version

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
from distutils.cmd import Command
import os

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
        path = "docs/_build/%s" % __version__
        mode = "html"

        try:
            os.makedirs(path)
        except:
            pass

        import os
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

# not officially supported, but included for flexibility in test environments
open_core_version = bool(os.environ.get('DSE_DRIVER_INSTALL_OPEN_CORE_VERSION'))
if open_core_version:
    dependencies = ['cassandra-driver >= 3.2.0a1']
else:
    dependencies = ['cassandra-driver == %s' % (_core_driver_target_version,)]

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
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
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
    license="DataStax DSE Driver License http://www.datastax.com/terms/datastax-dse-driver-license-terms",
    cmdclass={'doc': DocCommand})
