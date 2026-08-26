from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as arq:
    readme = arq.read()

setup(
    name="servify",
    version="0.1.11",
    license="MIT",
    author="Felipe Pegoraro",
    author_email="felipepegoraro93@gmail.com",
    description="A Python library that simplifies data manipulation and workflow development with PySpark in Databricks environments.",
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords="databricks spark pyspark",
    packages=find_packages(include=["servify", "servify.*"]),
    install_requires=[
        "loguru>=0.7.3",
        "holidays>=0.88",
        "pandas>=2.3.2",
        "pyarrow>=21.0.0",
        "tqdm>=4.67.1",
        "openpyxl>=3.1.2",
        "chardet>=5.2.0",
    ],
)
