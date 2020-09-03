# Copyright 2018-2020 Zegami Ltd

"""A command line interface for managing Zegami."""

from os import path

from setuptools import setup


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='zegami-cli',
    version='1.2.0',
    description='Command Line Interface for Zegami',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/zegami/zegami-cli',
    author='Zegami',
    author_email='help@zegami.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: User Interfaces',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=['zeg', 'zeg.tests'],
    install_requires=[
        'appdirs==1.4.3',
        'azure-storage-blob==12.3.0',
        'colorama==0.4.3',
        'jsonschema==3.2.0',
        'PyYaml==5.3.1',
        'requests<3.0,>=2.15.0',
        'tqdm==4.43.0',
    ],
    extras_require={
        'sql': [
            'pyodbc==4.0.30',
            'SQLAlchemy==1.3.15',
        ],
    },
    entry_points={
        'console_scripts': [
            'zeg=zeg.__main__:main'
        ]
    },
    project_urls={
        'Zegami': 'https://zegami.com',
        'Bug Reports': 'https://github.com/zegami/zegami-cli/issues',
    },
    include_package_data=True,
)
