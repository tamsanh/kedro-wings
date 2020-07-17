from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="kedro-wings",
    version="0.2.5",
    url="https://github.com/tamsanh/kedro-wings.git",
    author="Tam-Sanh Nguyen",
    author_email="tamsanh@gmail.com",
    description="Kedro Wings automatically creates catalog entries to simplify Kedro pipeline writing.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    license="MIT",
    install_requires=[
        'kedro>=0.16.0',
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
    ],
)
