-- +goose Up
-- +goose StatementBegin
-- Migration script to create the get_dependencies function with depth protection
DROP FUNCTION IF EXISTS get_dependencies(integer, varchar, varchar);

-- Create the function
CREATE FUNCTION get_dependencies(
    p_from_resource_proto_type integer,
    p_from_resource_name character varying,
    p_from_resource_variant character varying
) 
RETURNS TABLE (
    to_resource_proto_type integer,
    to_resource_name character varying,
    to_resource_variant character varying
) 
LANGUAGE plpgsql 
AS $function$
BEGIN
    RETURN QUERY
    WITH RECURSIVE dependency_chain AS (
        -- Base case: start from the given resource
        SELECT 
            e.from_resource_proto_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_proto_type,
            e.to_resource_name,
            e.to_resource_variant,
            1 AS depth -- Initialize depth
        FROM edges e
        WHERE e.from_resource_proto_type = p_from_resource_proto_type
            AND e.from_resource_name = p_from_resource_name
            AND e.from_resource_variant = p_from_resource_variant
        
        UNION ALL
        
        -- Recursive step: traverse downstream dependencies
        SELECT 
            e.from_resource_proto_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_proto_type,
            e.to_resource_name,
            e.to_resource_variant,
            dc.depth + 1 -- Increment depth
        FROM edges e
        INNER JOIN dependency_chain dc 
            ON e.from_resource_proto_type = dc.to_resource_proto_type
            AND e.from_resource_name = dc.to_resource_name
            AND e.from_resource_variant = dc.to_resource_variant
        WHERE dc.depth < 500 -- Limit recursion depth
    )
SELECT DISTINCT
    dc.to_resource_proto_type,
    dc.to_resource_name,
    dc.to_resource_variant
FROM dependency_chain dc;

-- Note: Explicit cycle detection logic is handled in the WHERE clause above
END;
$function$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Drop the function
DROP FUNCTION IF EXISTS get_dependencies(integer, varchar, varchar);
-- +goose StatementEnd