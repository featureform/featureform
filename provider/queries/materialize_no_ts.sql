-- If the schema lacks a timestamp, we assume each entity only has single entry. The
-- below query enforces this assumption by:
-- 1. Adding a row number to each row (this currently relies on the implicit ordering of the rows) - order_rows CTE
-- 2. Grouping by entity and selecting the max row number for each entity - max_row_per_entity CTE
-- 3. Joining the max row number back to the original table and selecting only the rows with the max row number - final select
WITH ordered_rows AS (
    SELECT %s AS entity,
        %s AS value,
        0 AS ts,
        ROW_NUMBER() over (
            PARTITION BY %s
            ORDER BY (
                    SELECT NULL
                )
        ) AS row_number
    FROM source_0
),
max_row_per_entity AS (
    SELECT entity,
        MAX(row_number) AS max_row
    FROM ordered_rows
    GROUP BY entity
)
SELECT ord.entity,
    ord.value,
    -- **NOTE:** It's critical to cast this to a timestamp despite the fact that it's a no-op;
    -- this is due to the fact that the materialization layer expects the timestamp column
    -- to be a timestamp type, otherwise it will fail.
    CAST(ord.ts AS TIMESTAMP) AS ts
FROM max_row_per_entity maxr
    JOIN ordered_rows ord ON ord.entity = maxr.entity
    AND ord.row_number = maxr.max_row
ORDER BY maxr.max_row DESC;