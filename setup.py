#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-userpilot",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="phani-yadla",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_userpilot"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests>=2.28.1"
    ],
    entry_points="""
    [console_scripts]
    target-userpilot=target_userpilot:main
    """,
    packages=["target_userpilot"],
    package_data = {},
    include_package_data=True,
)
