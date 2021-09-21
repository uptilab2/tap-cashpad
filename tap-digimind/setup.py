#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-digimind",
    version="0.1.0",
    description="Singer.io tap for extracting data from digimind",
    author="Mounir Yahyaoui",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_digimind"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-digimind=tap_digimind:main
    """,
    packages=["tap_digimind"],
    package_data={
        'tap_digimind/schemas': [
            "*.json",
        ],
    },
    include_package_data=True,
)
