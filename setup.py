from setuptools import setup

with open("README.md", "r") as arq:
    readme = arq.read()

setup(name='my-libs-py',
    version='0.0.5',
    license='MIT License',
    author='Felipe Pegoraro',
    long_description=readme,
    long_description_content_type="text/markdown",
    author_email='felipepegoraro93@gmail.com',
    keywords='libs python',
    description=u'Wrapper para fins de estudo e cases de projetos pessoais',
    packages=['my_libs'],
    install_requires=['pyspark'],
)