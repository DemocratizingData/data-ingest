#!/usr/bin/env python

import os
from setuptools import setup


build_tag = os.getenv('BUILD_VERSION_TAG')


setup(
    name='susdingest',
    version='0.1' + (f'+{build_tag}' if build_tag else ''),
    description='Show Us The Data Ingestion Utilities',
    author='Arik Mitschang',
    author_email='arik@jhu.edu',
    packages=['susdingest'],
    package_data={
        "susdingest": ["sql/*.sql", "data/*.csv"],
    },
    entry_points={
        'console_scripts': ['susdingest=susdingest.cli:main'],
    },
    install_requires=[
        'sqlalchemy',
        'pymssql',
        'boto3',
        'networkx',
        'pandas',
        'pyyaml',
    ],
    python_requires='>=3.8',
    test_suite='nose.collector',
    tests_require=[
        'nose',
    ]
)
