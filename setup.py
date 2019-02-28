#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

VERSION = '0.1.5'
DESCRIPTION = 'disque client'

setup(
    name='pydisque',
    version=VERSION,
    description=DESCRIPTION,
    author='ybrs',
    license='MIT',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/ybrs/pydisque",
    author_email='aybars.badur@gmail.com',
    packages=['pydisque'],
    install_requires=['redis', 'six'],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
