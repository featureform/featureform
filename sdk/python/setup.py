# This Source Code Form is subject to the terms of the Mozilla Public
# License, v.2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import re
from setuptools import setup, find_packages
import sys
import os

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='embeddings', 
    version='0.0.1', 
    packages=[package for package in find_packages()
                if package.startswith('embeddings')], 
    long_description=read('README.md'),
    python_requires='>=3.5',
    install_requires=[
        'grpcio>=1.35.0',
        'Faker==4.18.0',
    ],
    author='FeatureForm',
    author_email='alexi@featureform.io, simba@featureform.io')