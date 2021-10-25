import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("pypi_requirements.txt", "r") as f:
    required = f.read().splitlines()

setuptools.setup(
    name="embeddinghub",
    version="0.0.1.post12",
    author="featureform",
    author_email="hello@featureform.com",
    description="Data infrastructure for machine learning embeddings.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=required,
    url="https://github.com/featureform/embeddinghub",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
)
