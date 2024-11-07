#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import setuptools

with open("README", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="embeddingstore",
    version="0.0.1",
    author="featureform",
    author_email="hello@featureform.com",
    description="Data infrastructure for machine learning embeddings.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/featureform/embeddingstore",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "sdk/python"},
    packages=setuptools.find_packages(where="sdk/python"),
    python_requires=">=3.6",
)
