-- **NOTE** It's necessary to keep the 3 columns to ensure that materialization to
-- DynamoDB via S3 import will succeed; additional columns will require changes to
-- the S3 import process and given any additional columns won't impact feature serving,
-- they should be excluded from the SELECT statement.
SELECT entity,
    value,
    ts
FROM (
        SELECT entity,
            value,
            ts,
            ROW_NUMBER() OVER (
                PARTITION BY entity
                ORDER BY ts DESC
            ) AS rn2
        FROM (
                SELECT entity,
                    value,
                    ts,
                    rn
                FROM (
                        SELECT %s AS entity,
                            %s AS value,
                            %s AS ts,
                            ROW_NUMBER() OVER (
                                ORDER BY (
                                        SELECT NULL
                                    )
                            ) AS rn
                        FROM %s
                    ) t
                ORDER BY rn DESC
            ) t2
    ) t3
WHERE rn2 = 1;