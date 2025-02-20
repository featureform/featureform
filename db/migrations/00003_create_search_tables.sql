-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS search_resources (
    id            TEXT PRIMARY KEY,
    name          TEXT,
    type          TEXT,
    variant       TEXT,
    tags          TEXT[],
    search_vector tsvector,
    created_at    TIMESTAMP DEFAULT now(),
    updated_at    TIMESTAMP DEFAULT now()
);

-- Create GIN index for full-text search
CREATE INDEX IF NOT EXISTS resources_search_idx
    ON search_resources USING GIN (search_vector);

-- Create function to automatically update search_vector
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', coalesce(NEW.name, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.type, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(NEW.variant, '')), 'C') ||
        setweight(to_tsvector('english', coalesce(array_to_string(NEW.tags, ' '), '')), 'D');
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for search vector updates
CREATE TRIGGER resources_search_vector_update
    BEFORE INSERT OR UPDATE ON search_resources
    FOR EACH ROW
    EXECUTE FUNCTION update_search_vector();

-- Function to sync ff_task_metadata to search_resources
CREATE OR REPLACE FUNCTION sync_to_search_resources()
RETURNS TRIGGER AS $$
DECLARE
    message_json jsonb;
BEGIN
    -- First parse the outer JSON to get the Message field
    message_json := (NEW.value::jsonb->>'Message')::jsonb;
    
    INSERT INTO search_resources (
        id,
        name,
        type,
        variant,
        tags
    )
    SELECT
        REGEXP_REPLACE(NEW.key, '[@.\s]', '_', 'g'),
        SPLIT_PART(NEW.key, '__', 2),                 -- Get name (middle part)
        SPLIT_PART(NEW.key, '__', 1),                 -- Get type (first part)
        COALESCE(NULLIF(SPLIT_PART(NEW.key, '__', 3), ''), ''),  -- Get variant (last part)
        CASE 
            WHEN message_json->'tags'->'tag' IS NOT NULL THEN
                (SELECT array_agg(elem::text)
                 FROM jsonb_array_elements_text(message_json->'tags'->'tag') AS elem)
            ELSE
                ARRAY[]::TEXT[]
        END
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        variant = EXCLUDED.variant,
        tags = EXCLUDED.tags,
        updated_at = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for syncing to search_resources
CREATE TRIGGER after_ff_task_metadata_change
    AFTER INSERT OR UPDATE ON ff_task_metadata
    FOR EACH ROW
    EXECUTE FUNCTION sync_to_search_resources();

-- Populate search_resources with existing ff_task_metadata data
DO $$
DECLARE
    message_json jsonb;
BEGIN
    INSERT INTO search_resources (
        id,
        name,
        type,
        variant,
        tags
    )
    SELECT DISTINCT
        REGEXP_REPLACE(key, '[@.\s]', '_', 'g') as id,
        SPLIT_PART(key, '__', 2) as name,
        SPLIT_PART(key, '__', 1) as type,
        COALESCE(NULLIF(SPLIT_PART(key, '__', 3), ''), '') as variant,
        CASE 
            WHEN (value::jsonb->>'Message')::jsonb->'tags'->'tag' IS NOT NULL THEN
                (SELECT array_agg(elem::text)
                 FROM jsonb_array_elements_text(((value::jsonb->>'Message')::jsonb->'tags'->'tag')) AS elem)
            ELSE
                ARRAY[]::TEXT[]
        END as tags
    FROM ff_task_metadata
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        variant = EXCLUDED.variant,
        tags = EXCLUDED.tags,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Drop trigger, function, index, and table in the reverse order of creation
DROP TRIGGER IF EXISTS resources_search_vector_update ON search_resources;
DROP FUNCTION IF EXISTS update_search_vector();
DROP INDEX IF EXISTS resources_search_idx;
DROP TABLE IF EXISTS search_resources;
DROP TRIGGER IF EXISTS after_ff_task_metadata_change ON ff_task_metadata;
DROP FUNCTION IF EXISTS sync_to_search_resources();
-- +goose StatementEnd