"""
Setup configuration for NRS (NICU Analytics Pipeline) package.
"""
from setuptools import setup, find_packages

with open("NRS/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nrs",
    version="1.0.0",
    author="Analytics Team",
    description="NICU Analytics Pipeline for medical claims data processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "snowflake-snowpark-python>=1.34.0",
        "cryptography>=3.4.8",
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "python-dateutil>=2.8.2",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
