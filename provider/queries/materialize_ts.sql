SELECT %s as entity,
    %s as value,
    %s as ts
FROM %s t1
WHERE t1.%s = (
        SELECT MAX(t2.%s)
        FROM %s t2
        WHERE t1.%s = t2.%s
    );