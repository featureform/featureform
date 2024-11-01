#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

FROM python:3.10-slim

WORKDIR /usr/app/src

COPY provider/scripts/k8s/offline_store_pandas_runner.py ./
COPY provider/scripts/k8s/requirements.txt ./

RUN pip install -r ./requirements.txt

CMD [ "python", "./offline_store_pandas_runner.py"]