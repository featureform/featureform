CREATE TABLE transactions (
  TransactionID        TEXT,
  CustomerID           TEXT NOT NULL,
  CustomerDOB          DATE,
  CustLocation         TEXT,
  CustAccountBalance   NUMERIC(12,2),
  TransactionAmount    NUMERIC(12,2),
  "Timestamp"          DATETIME,
  IsFraud              BOOLEAN,
)
ENGINE MergeTree()
PRIMARY KEY TransactionID;

CREATE TEMPORARY TABLE staging_transactions (
  TransactionID       TEXT,
  CustomerID          TEXT,
  CustomerDOB         TEXT,
  CustLocation        TEXT,
  CustAccountBalance  NUMERIC(12,2),
  TransactionAmount   NUMERIC(12,2),
  "Timestamp"         TEXT,
  IsFraud             TEXT
);

INSERT INTO staging_transactions FROM INFILE '/docker-entrypoint-initdb.d/transactions.csv' FORMAT CSV;

INSERT INTO transactions (
  TransactionID,
  CustomerID,
  CustomerDOB,
  CustLocation,
  CustAccountBalance,
  TransactionAmount,
  "Timestamp",
  IsFraud
)
SELECT
  TransactionID,
  CustomerID,
  CASE
    -- If DOB matches pattern DD/MM/YY, parse it:
    WHEN match(CustomerDOB,'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$') THEN
      CASE
        WHEN parseDateTimeBestEffort(CustomerDOB) > today()
          THEN parseDateTimeBestEffort(CustomerDOB) - INTERVAL '100 years'
        ELSE parseDateTimeBestEffort(CustomerDOB)
      END
    ELSE
      -- If DOB is 'NaN', '.', empty, or doesn't match pattern, store NULL
      NULL
  END
    AS parsed_DOB,
  CustLocation,
  CustAccountBalance,
  TransactionAmount,
  -- Example format: "2022-04-12 12:52:20 UTC"
  -- Parse with to_timestamp, then treat as UTC
  parseDateTimeBestEffort("Timestamp")
    AS parsed_timestamp,
  CASE
    WHEN lower(IsFraud) = 'true'  THEN true
    WHEN lower(IsFraud) = 'false' THEN false
    ELSE false
  END
    AS parsed_fraud
FROM staging_transactions;

DROP TABLE staging_transactions;