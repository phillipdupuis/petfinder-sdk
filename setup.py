from setuptools import setup, find_packages


def readme():
    with open("README.md", "r") as infile:
        return infile.read()


classifiers = [
    "Development Status :: 4 - Beta",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
]

install_requires = [
    "httpx",
    "pydantic",
]

setup(
    name="petfinder-client",
    version="0.0.1",
    description="Petfinder API client",
    license="MIT",
    long_description=readme(),
    long_description_content_type="text/markdown",
    keywords=["async"],
    author="Phillip Dupuis",
    author_email="phillip_dupuis@alumni.brown.edu",
    url="https://github.com/phillipdupuis/petfinder-client",
    install_requires=install_requires,
    packages=find_packages(),
    classifiers=classifiers,
)
