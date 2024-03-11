# -*- coding: utf-8 -*-
import os

import setuptools  # type: ignore

package_root = os.path.abspath(os.path.dirname(__file__))

name = "cloudquery-plugin-sdk"

description = "CloudQuery Plugin SDK for Python"

dependencies = [
    "cloudquery-plugin-pb==0.0.24",
    "exceptiongroup==1.2.0",
    "black==24.2.0",
    "grpcio==1.62.0",
    "grpcio-tools==1.62.0",
    "iniconfig==2.0.0",
    "Jinja2==3.1.3",
    "MarkupSafe==2.1.5",
    "numpy==1.26.4",
    "packaging==23.2",
    "pandas==2.2.1",
    "pluggy==1.4.0",
    "protobuf==4.25.3",
    "pyarrow==15.0.0",
    "pytest==8.0.2",
    "python-dateutil==2.8.2",
    "pytz==2023.4",
    "six==1.16.0",
    "structlog==23.3.0",
    "tomli==2.0.1",
    "tzdata==2023.4",
]
url = "https://github.com/cloudquery/plugin-sdk-python"

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
    version="0.1.16",
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
