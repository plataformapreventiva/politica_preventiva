#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from os import path

"""
politica_preventiva-pipeline: Pipeline politica_preventiva
"""

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md')) as f:    
    long_description = f.read()

setup(
    name="politica_preventiva",
    version='0.0.1',
    description='Pipeline politica_preventiva',
    long_description=long_description,
    author_email='roberto.sancheza@sedesol.gob.mx',
    license='GPL v3',

    packages=find_packages(),

    test_suite='tests',

    install_requires=[
        'numpy',
        'pyyaml',
        'pandas',
        'scikit-learn',
        'datetime',
        'scipy',
        'Click',
        'boto3'
    ],

    extra_require={
        'test': ['mock']
    },

    entry_points = {
        'console_scripts' : [
            'politica_preventiva = politica_preventiva.scripts.cli:main',

        ]
    },

    zip_safe=False
)

