--
-- init.sql
--

--------------------------------------------------
-- 1) Create the final table (with a DOB check)
--------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
  TransactionID        TEXT PRIMARY KEY,
  CustomerID           TEXT NOT NULL,
  CustomerDOB          DATE,
  CustLocation         TEXT,
  CustAccountBalance   NUMERIC(12,2),
  TransactionAmount    NUMERIC(12,2),
  "Timestamp"          TIMESTAMPTZ,
  IsFraud              BOOLEAN,
  CONSTRAINT check_past_dob CHECK (CustomerDOB < CURRENT_DATE)
);

--------------------------------------------------
-- 2) Create a *temporary staging table*:
--    - We load CSV columns as TEXT (for DOB and Timestamp)
--    - Then we'll convert them properly when inserting into final table
--------------------------------------------------
CREATE TEMP TABLE staging_transactions (
  TransactionID       TEXT,
  CustomerID          TEXT,
  CustomerDOB         TEXT,
  CustLocation        TEXT,
  CustAccountBalance  NUMERIC(12,2),
  TransactionAmount   NUMERIC(12,2),
  "Timestamp"         TEXT,
  IsFraud             TEXT
);

--------------------------------------------------
-- 3) COPY the CSV into the staging table
--    (the CSV must have matching header columns).
--    This runs when the container initializes
--    if these files are placed in /docker-entrypoint-initdb.d/
--------------------------------------------------
COPY staging_transactions (
  TransactionID,
  CustomerID,
  CustomerDOB,
  CustLocation,
  CustAccountBalance,
  TransactionAmount,
  "Timestamp",
  IsFraud
)
FROM '/docker-entrypoint-initdb.d/transactions.csv'
CSV HEADER;

--------------------------------------------------
-- 4) Insert into final table, parsing/fixing the data
--    - Use 'DD/MM/RR' to interpret 2-digit years so '73' → '1973'.
--    - If the parsed date would exceed today's date, subtract 100 years.
--    - Convert the "Timestamp" string to timestamptz.
--    - Convert "IsFraud" from text to boolean.
--------------------------------------------------
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
    WHEN CustomerDOB ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' THEN
      CASE
        WHEN to_date(CustomerDOB, 'DD/MM/RR') > CURRENT_DATE
          THEN to_date(CustomerDOB, 'DD/MM/RR') - INTERVAL '100 years'
        ELSE to_date(CustomerDOB, 'DD/MM/RR')
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
  (to_timestamp("Timestamp", 'YYYY-MM-DD HH24:MI:SS "UTC"')
   AT TIME ZONE 'UTC')
    AS parsed_timestamp,
  CASE 
    WHEN lower(IsFraud) = 'true'  THEN true
    WHEN lower(IsFraud) = 'false' THEN false
    ELSE false
  END
    AS parsed_fraud
FROM staging_transactions;

--------------------------------------------------
-- 5) Drop staging table if you don't need it anymore
--------------------------------------------------
DROP TABLE staging_transactions;
