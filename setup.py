#!/usr/bin/env python
import os
import re

from setuptools import setup, find_packages

# Read requirements.txt
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="databricks_uniform_sync",
    version="0.1.0",
    description="A SDK for syncing Databricks using Unity Catalog and Uniform",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Guanjie Shen",
    url="https://github.com/guanjieshen/databricks-uniform-sync",
    packages=find_packages(exclude=["tests*"]),
    install_requires=requirements,
    license="MIT License",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
    ],
)