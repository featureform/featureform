#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE fraud;
    CREATE TABLE fraud.creditcard
    (
        id UInt64,
        V1 Float64,
        V2 Float64,
        V3 Float64,
        V4 Float64,
        V5 Float64,
        V6 Float64,
        V7 Float64,
        V8 Float64,
        V9 Float64,
        V10 Float64,
        V11 Float64,
        V12 Float64,
        V13 Float64,
        V14 Float64,
        V15 Float64,
        V16 Float64,
        V17 Float64,
        V18 Float64,
        V19 Float64,
        V20 Float64,
        V21 Float64,
        V22 Float64,
        V23 Float64,
        V24 Float64,
        V25 Float64,
        V26 Float64,
        V27 Float64,
        V28 Float64,
        Amount Float64,
        Class UInt8
    )
    ENGINE = MergeTree
    ORDER BY Class;
    INSERT INTO fraud.creditcard FROM INFILE '/docker-entrypoint-initdb.d/credit_card_2023.csv.gz';
EOSQL
