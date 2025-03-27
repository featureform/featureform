-- +goose Up
-- +goose StatementBegin

-- Update process_label_variant to handle entity and user relationships
CREATE OR REPLACE FUNCTION process_label_variant(label_variant_key text, label_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    label_type        INT := 5;  -- Resource type for label_variant
    source_type       INT := 7;  -- Resource type for source_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    provider_type     INT := 8;  -- Resource type for provider
    entity_type       INT := 9;  -- Resource type for entity
    user_type         INT := 11; -- Resource type for user
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in JSON arrays
BEGIN
    message := (label_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge to provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;

    -- Add edge to source
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;

    -- Add edge from entity to label
    IF (message ->> 'entity') IS NOT NULL THEN
        PERFORM add_edge(
            entity_type,
            message ->> 'entity',
            '',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    ELSIF message -> 'entityMappings' -> 'mappings' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'entityMappings' -> 'mappings')
        LOOP
            PERFORM add_edge(
                entity_type,
                item ->> 'name',
                '',
                label_type,
                parse_resource_key(label_variant_key, 'name'),
                parse_resource_key(label_variant_key, 'variant')
            );
        END LOOP;
    END IF;

    -- Add edge from user to label
    IF (message ->> 'owner') IS NOT NULL THEN
        PERFORM add_edge(
            user_type,
            message ->> 'owner',
            '',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Update process_feature_variant to handle entity and user relationships
CREATE OR REPLACE FUNCTION process_feature_variant(feature_variant_key text, feature_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    feature_type      INT := 4;  -- Resource type for feature_variant
    provider_type     INT := 8;  -- Resource type for provider
    source_type       INT := 7;  -- Resource type for source_variant
    entity_type       INT := 9;  -- Resource type for entity
    user_type         INT := 11; -- Resource type for user
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in the array
BEGIN
    message := (feature_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from provider to the feature
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;

    -- Add edge from source to the feature
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;

    -- Add edge from entity to feature
    IF (message ->> 'entity') IS NOT NULL THEN
        PERFORM add_edge(
            entity_type,
            message ->> 'entity',
            '',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;

    -- Add edge from user to feature
    IF (message ->> 'owner') IS NOT NULL THEN
        PERFORM add_edge(
            user_type,
            message ->> 'owner',
            '',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Update process_ts_variant to handle user relationships
CREATE OR REPLACE FUNCTION process_ts_variant(ts_key TEXT, ts_value TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    ts_type       INT := 6;  -- Resource type for TS_VARIANT
    provider_type INT := 8;  -- Resource type for provider
    label_type    INT := 5;  -- Resource type for label_variant
    feature_type  INT := 4;  -- Resource type for feature_variant
    user_type     INT := 11; -- Resource type for user
    message       JSONB;     -- Parsed Message JSON
    item          JSONB;     -- Individual item in lists
BEGIN
    message := (ts_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from TS_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add edge from TS_VARIANT to its label
    IF message ->> 'label' IS NOT NULL THEN
        PERFORM add_edge(
            label_type,
            ((message ->> 'label')::jsonb) ->> 'name',
            ((message ->> 'label')::jsonb) ->> 'variant',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add edges for features
    IF message -> 'features' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'features')
        LOOP
            PERFORM add_edge(
                feature_type,
                item ->> 'name',
                item ->> 'variant',
                ts_type,
                parse_resource_key(ts_key, 'name'),
                parse_resource_key(ts_key, 'variant')
            );
        END LOOP;
    END IF;

    -- Add edge from user to training set
    IF (message ->> 'owner') IS NOT NULL THEN
        PERFORM add_edge(
            user_type,
            message ->> 'owner',
            '',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;
END;
$$;

-- Update process_source_variant to handle user relationships
CREATE OR REPLACE FUNCTION process_source_variant(source_variant_key text, source_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    source_type       INT := 7;  -- Resource type for source_variant
    provider_type     INT := 8;  -- Resource type for provider
    feature_type      INT := 4;  -- Resource type for feature_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    label_type        INT := 5;  -- Resource type for label_variant
    user_type         INT := 11; -- Resource type for user
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in lists
BEGIN
    message := (source_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from its provider to the SOURCE_VARIANT
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            source_type,
            parse_resource_key(source_variant_key, 'name'),
            parse_resource_key(source_variant_key, 'variant')
        );
    END IF;

    -- Process the "source" array in SQLTransformation or DFTransformation
    IF message -> 'transformation' -> 'SQLTransformation' -> 'source' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'SQLTransformation' -> 'source')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    ELSIF message -> 'transformation' -> 'DFTransformation' -> 'inputs' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'DFTransformation' -> 'inputs')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    END IF;

    -- Add edge from user to source
    IF (message ->> 'owner') IS NOT NULL THEN
        PERFORM add_edge(
            user_type,
            message ->> 'owner',
            '',
            source_type,
            parse_resource_key(source_variant_key, 'name'),
            parse_resource_key(source_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Reprocess all entries to create new edges
DO $$
BEGIN
    PERFORM add_edges_from_all_ff_task_metadata();
END;
$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Restore original process_label_variant function
CREATE OR REPLACE FUNCTION process_label_variant(label_variant_key text, label_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    label_type        INT := 5;  -- Resource type for label_variant
    source_type       INT := 7;  -- Resource type for source_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    provider_type     INT := 8;  -- Resource type for provider
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in JSON arrays
BEGIN
    message := (label_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge to provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;

    -- Add edge to source
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Restore original process_feature_variant function
CREATE OR REPLACE FUNCTION process_feature_variant(feature_variant_key text, feature_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    feature_type      INT := 4;  -- Resource type for feature_variant
    provider_type     INT := 8;  -- Resource type for provider
    training_set_type INT := 6;  -- Resource type for training_set_variant
    source_type       INT := 7;  -- Resource type for source_variant
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in the array
BEGIN
    message := (feature_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from FEATURE_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;

    -- Add edge from FEATURE_VARIANT to its source
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Restore original process_ts_variant function
CREATE OR REPLACE FUNCTION process_ts_variant(ts_key TEXT, ts_value TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    ts_type       INT := 6;  -- Resource type for TS_VARIANT
    provider_type INT := 8;  -- Resource type for provider
    label_type    INT := 5;  -- Resource type for label_variant
    feature_type  INT := 4;  -- Resource type for feature_variant
    message       JSONB;     -- Parsed Message JSON
    item          JSONB;     -- Individual item in lists
BEGIN
    message := (ts_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from TS_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add edge from TS_VARIANT to its label
    IF message ->> 'label' IS NOT NULL THEN
        PERFORM add_edge(
            label_type,
            ((message ->> 'label')::jsonb) ->> 'name',
            ((message ->> 'label')::jsonb) ->> 'variant',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add edges for features
    IF message -> 'features' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'features')
        LOOP
            PERFORM add_edge(
                feature_type,
                item ->> 'name',
                item ->> 'variant',
                ts_type,
                parse_resource_key(ts_key, 'name'),
                parse_resource_key(ts_key, 'variant')
            );
        END LOOP;
    END IF;
END;
$$;

-- Restore original process_source_variant function
CREATE OR REPLACE FUNCTION process_source_variant(source_variant_key text, source_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    source_type       INT := 7;  -- Resource type for source_variant
    provider_type     INT := 8;  -- Resource type for provider
    feature_type      INT := 4;  -- Resource type for feature_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    label_type        INT := 5;  -- Resource type for label_variant
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in lists
BEGIN
    message := (source_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge from its provider to the SOURCE_VARIANT
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            source_type,
            parse_resource_key(source_variant_key, 'name'),
            parse_resource_key(source_variant_key, 'variant')
        );
    END IF;

    -- Process the "source" array in SQLTransformation or DFTransformation
    IF message -> 'transformation' -> 'SQLTransformation' -> 'source' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'SQLTransformation' -> 'source')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    ELSIF message -> 'transformation' -> 'DFTransformation' -> 'inputs' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'DFTransformation' -> 'inputs')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    END IF;
END;
$$;

-- Remove entity and user edges
DELETE FROM edges 
WHERE from_resource_proto_type IN (9, 11)  -- Entity and User types
   OR to_resource_proto_type IN (9, 11);   -- Entity and User types

-- +goose StatementEnd
