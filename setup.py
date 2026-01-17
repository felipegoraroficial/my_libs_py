from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as arq:
    readme = arq.read()

setup(
    name="my-libs-py",
    version="0.0.11",
    license="MIT",
    author="Felipe Pegoraro",
    author_email="felipepegoraro93@gmail.com",
    description="Commons utilitários para projetos Spark",
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords="spark pyspark utils commons",
    packages=find_packages(include=["my_libs", "app", "app.*"]),
    install_requires=[
        "pyspark>=4.0.0",
        "delta-spark>=4.0.1",
        "loguru>=0.7.3",
        "holidays>=0.88",
        "pandas>=2.3.2",  # só se o commons realmente usa
        "pyarrow>=21.0.0",  # só se o commons realmente usa
    ],
)
