#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-mysql"
package_version = "0.0.1"
description = """The mysql adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author=None,
    author_email=None,
    url=None,
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/mysql/macros/*.sql',
            'include/mysql/dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt-core==0.16.1",
        "mysql-connector-python==8.0.19"
    ]
)
