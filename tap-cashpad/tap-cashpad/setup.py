#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-cashpad",
    version="0.9.0",
    description="Singer Cashpad tap for extracting data",
    author="Mounir Yahyaoui",
    url="http://reeport.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_cashpad"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-cashpad=tap_cashpad:main
    """,
    packages=["tap_cashpad"],
    package_data={
        "schemas": ["tap_cashpad/schemas/*.json"]
    },
    include_package_data=True,
)
