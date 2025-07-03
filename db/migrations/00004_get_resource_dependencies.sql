-- +goose Up
-- +goose StatementBegin

-- Function: get_dependencies_by_type
CREATE OR REPLACE FUNCTION get_dependencies_by_type(
    p_from_resource_proto_type integer,
    p_from_resource_name character varying,
    p_from_resource_variant character varying,
    p_resource_type integer
)
RETURNS TABLE (
    key VARCHAR(2048),
    value text
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ftm.key,
        ftm.value
    FROM get_dependencies(
        p_from_resource_proto_type,
        p_from_resource_name,
        p_from_resource_variant
    ) d
    JOIN ff_task_metadata ftm ON ftm.key = create_resource_key(
        d.to_resource_proto_type,
        d.to_resource_name,
        d.to_resource_variant
    )
    WHERE d.to_resource_proto_type = p_resource_type
        AND ftm.marked_for_deletion_at IS NULL;
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION IF EXISTS get_dependencies_by_type(integer, varchar, varchar, integer);

-- +goose StatementEnd
