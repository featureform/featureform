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
    install_requires=[
        
    ],
    author='FeatureForm',
    author_email='alexi@featureform.io, simba@featureform.io')