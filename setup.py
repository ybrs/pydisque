#!/usr/bin/env python

from setuptools import setup

VERSION = '0.1'
DESCRIPTION = 'disque client'

setup(
    name='python-disque',
    version=VERSION,
    description=DESCRIPTION,
    author='ybrs',
    license='MIT',
    url="http://github.com/ybrs/mongomodels",
    author_email='aybars.badur@gmail.com',
    packages=['disque'],
    install_requires=['redis'],
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