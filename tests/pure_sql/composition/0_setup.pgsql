CREATE TABLE streams (
    stream_id text NOT NULL,
    data_provider text NOT NULL,
    stream_type text NOT NULL,

    -- Primary key must be defined inline
    PRIMARY KEY (data_provider, stream_id),

    -- Constraints
    CHECK (stream_type IN ('primitive', 'composed'))
);

-- Create indexes separately
CREATE INDEX stream_type_composite_idx ON streams (stream_type, data_provider, stream_id);

CREATE TABLE taxonomies (
    data_provider text NOT NULL,
    stream_id text NOT NULL,
    taxonomy_id UUID NOT NULL,
    child_stream_id text NOT NULL,
    child_data_provider text NOT NULL,
    weight NUMERIC(36, 18) NOT NULL,
    created_at BIGINT NOT NULL,
    disabled_at BIGINT, --disabled at is referring to the block at which is disabled. not related to start time
    version BIGINT NOT NULL,
    start_time BIGINT NOT NULL,

    PRIMARY KEY (taxonomy_id),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    -- won't add foreign key for child_data_provider, child_stream_id
    -- because we don't want to taxonomies to change if a stream is deleted

    CHECK (weight >= 0),
    CHECK (version >= 0),
    CHECK (start_time >= 0)
);

-- Create indexes separately
CREATE INDEX child_stream_idx ON taxonomies (data_provider, stream_id, start_time, version, child_data_provider, child_stream_id);

-- Function to create a stream
CREATE OR REPLACE FUNCTION create_stream(
    p_stream_id text,
    p_data_provider text,
    p_stream_type text
) RETURNS void AS
$$
BEGIN
    INSERT INTO streams (stream_id, data_provider, stream_type)
    VALUES (p_stream_id, p_data_provider, p_stream_type);
END;
$$ LANGUAGE plpgsql;

-- Function to get latest active version
CREATE OR REPLACE FUNCTION get_latest_active_version(
    p_data_provider text,
    p_stream_id text
) RETURNS BIGINT AS
$$
DECLARE
    result_version BIGINT;
BEGIN
    SELECT COALESCE(MAX(taxonomies.version), 0) INTO result_version
    FROM taxonomies
    WHERE taxonomies.data_provider = p_data_provider
    AND taxonomies.stream_id = p_stream_id;
    
    RETURN result_version;
END;
$$ LANGUAGE plpgsql;

-- Creates a set of taxonomies for a given stream, with 1 weight
CREATE OR REPLACE FUNCTION create_taxonomy(
    p_data_provider text,
    p_stream_id text,
    p_child_stream_ids text[],
    p_child_data_providers text[],
    p_start_time BIGINT,
    p_disabled_at BIGINT
) RETURNS void AS
$$
DECLARE
    weight numeric(36, 18);
    version_num BIGINT;
    i int;
    current_time_val BIGINT;
BEGIN
    weight := 1;
    
    -- Get the latest version and increment it
    version_num := get_latest_active_version(p_data_provider, p_stream_id) + 1;
    
    -- Get current timestamp as BIGINT
    current_time_val := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT;
    
    -- Make sure arrays have the same length
    IF array_length(p_child_stream_ids, 1) != array_length(p_child_data_providers, 1) THEN
        RAISE EXCEPTION 'Arrays child_stream_ids and child_data_providers must have the same length';
    END IF;
    
    FOR i IN 1..array_length(p_child_stream_ids, 1) LOOP
        INSERT INTO taxonomies (
            data_provider, 
            stream_id, 
            taxonomy_id, 
            child_stream_id, 
            child_data_provider, 
            weight, 
            created_at, 
            disabled_at, 
            version, 
            start_time
        ) VALUES (
            p_data_provider, 
            p_stream_id, 
            gen_random_uuid(), 
            p_child_stream_ids[i], 
            p_child_data_providers[i], 
            weight, 
            current_time_val, 
            p_disabled_at, 
            version_num, 
            p_start_time
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;