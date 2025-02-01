-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS search_resources (
    id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    variant TEXT,
    tags TEXT[],
    search_vector tsvector,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Create GIN index for full-text search
CREATE INDEX IF NOT EXISTS resources_search_idx ON search_resources USING GIN (search_vector);

-- Create function to automatically update search_vector
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', coalesce(NEW.name,'')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.type,'')), 'B') ||
        setweight(to_tsvector('english', coalesce(NEW.variant,'')), 'C') ||
        setweight(to_tsvector('english', coalesce(array_to_string(NEW.tags, ' '),'')), 'D');
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for search vector updates
CREATE TRIGGER resources_search_vector_update
    BEFORE INSERT OR UPDATE ON search_resources
    FOR EACH ROW
    EXECUTE FUNCTION update_search_vector();

CREATE TABLE IF NOT EXISTS ff_ordered_id (
    namespace VARCHAR(2048) PRIMARY KEY,
    current_id BIGINT
);

CREATE TABLE IF NOT EXISTS ff_task_metadata (
    key VARCHAR(2048) PRIMARY KEY,
    value TEXT,
    marked_for_deletion_at TIMESTAMP DEFAULT null
);

CREATE INDEX ff_key_pattern ON ff_task_metadata (key text_pattern_ops);

CREATE TABLE IF NOT EXISTS ff_locks (
    owner VARCHAR(255),
    key VARCHAR(2048) NOT NULL,
    expiration TIMESTAMP NOT NULL,
    PRIMARY KEY (key)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS search_resources_search_vector_update ON search_resources;
DROP FUNCTION IF EXISTS update_search_vector();
DROP INDEX IF EXISTS resources_search_idx;
DROP TABLE IF EXISTS ff_locks;
DROP TABLE IF EXISTS ff_task_metadata;
DROP TABLE IF EXISTS ff_ordered_id;
DROP TABLE IF EXISTS search_resources;
-- +goose StatementEnd