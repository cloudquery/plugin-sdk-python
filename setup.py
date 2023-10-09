# -*- coding: utf-8 -*-
import os

import setuptools  # type: ignore

package_root = os.path.abspath(os.path.dirname(__file__))

name = "cloudquery-plugin-sdk"

description = "CloudQuery Plugin SDK for Python"

dependencies = [
    "cloudquery-plugin-pb==0.0.17",
    "exceptiongroup==1.1.3",
    "black==23.9.1",
    "grpcio==1.59.0",
    "grpcio-tools==1.59.0",
    "iniconfig==2.0.0",
    "Jinja2==3.1.2",
    "MarkupSafe==2.1.3",
    "numpy==1.26.0",
    "packaging==23.1",
    "pandas==2.1.1",
    "pluggy==1.3.0",
    "protobuf==4.24.3",
    "pyarrow==12.0.1",
    "pytest==7.4.2",
    "python-dateutil==2.8.2",
    "pytz==2023.3.post1",
    "six==1.16.0",
    "structlog==23.1.0",
    "tomli==2.0.1",
    "tzdata==2023.3",
]
url = "https://github.com/cloudquery/plugin-sdk-python"

package_root = os.path.abspath(os.path.dirname(__file__))

long_description = """
CloudQuery Plugin SDK for Python
================================================

Overview
-----------

This is the high-level package to use for developing CloudQuery plugins in Python. See [github.com/cloudquery/plugin-sdk-python](https://github.com/cloudquery/plugin-sdk-python) for more information.
"""

packages = [
    package
    for package in setuptools.PEP420PackageFinder.find()
    if package.startswith("cloudquery")
]
setuptools.setup(
    name=name,
    version="0.1.5",
    description=description,
    long_description=long_description,
    author="CloudQuery LTD",
    author_email="pypi-packages@cloudquery.io",
    license="MPL-2.0",
    url=url,
    classifiers=[
        # release_status,
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Topic :: Internet",
    ],
    platforms="Posix; MacOS X; Windows",
    packages=packages,
    python_requires=">=3.7",
    namespace_packages=["cloudquery"],
    install_requires=dependencies,
    include_package_data=True,
    package_data={"cloudquery": ["sdk/py.typed"]},
    zip_safe=False,
)
