#Deriving the latest base image
FROM python:latest

WORKDIR /usr/app/src

COPY provider/scripts/k8s/offline_store_pandas_runner.py ./
COPY provider/scripts/k8s/requirements.txt ./

RUN pip install -r ./requirements.txt

CMD [ "python", "./offline_store_pandas_runner.py"]