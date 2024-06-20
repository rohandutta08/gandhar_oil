import setuptools


setuptools.setup(
    name="integration-transformation-layer",
    version="0.0.1",
    description="integration-transformation-layer",
    url="https://github.com/ClearTax/integration-transformation-layer",
    author="Vaibhav khantwal",
    author_email="vaibhav.khantwal@clear.in",
    license="MIT",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    package_data={
        "mappings": ["*.json"],
    },
)
