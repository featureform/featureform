FROM clickhouse/clickhouse-server:latest
ENV RUN_INITDB_SCRIPTS=true
COPY init-db.sh /docker-entrypoint-initdb.d/init-db.sh
RUN wget -O /docker-entrypoint-initdb.d/credit_card_2023.csv.gz 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/ml/creditcard_2023.csv.gz'
