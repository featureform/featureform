#Deriving the latest base image
FROM python:latest

WORKDIR /usr/app/src

COPY offline_store_pandas_runner.py ./
COPY requirements.txt ./

RUN pip install -r ./requirements.txt

CMD [ "python", "./offline_store_pandas_runner.py"]