-- First, create all the streams mentioned in the description
-- Using simplified names for better readability in tests

-- Create streams
SELECT create_stream('1c', 'provider1', 'composed');       -- 1c
SELECT create_stream('1.1c', 'provider1', 'composed');     -- 1.1c
SELECT create_stream('1.1.1p', 'provider1', 'primitive');  -- 1.1.1p
SELECT create_stream('1.1.2p', 'provider1', 'primitive');  -- 1.1.2p
SELECT create_stream('1.2c', 'provider1', 'composed');     -- 1.2c
SELECT create_stream('1.2.1p', 'provider1', 'primitive');  -- 1.2.1p
SELECT create_stream('1.3p', 'provider1', 'primitive');    -- 1.3p
SELECT create_stream('1.4c', 'provider1', 'composed');     -- 1.4c
SELECT create_stream('1.5p', 'provider1', 'primitive');    -- 1.5p

-- Now create the taxonomies for different time periods

-- start time 0:
-- stream 1c: with substreams 1.1c, 1.2c, 1.3p, 1.4c
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1c',          -- stream_id (1c)
    ARRAY[
        '1.1c',    -- 1.1c
        '1.2c',    -- 1.2c
        '1.3p',    -- 1.3p
        '1.4c'     -- 1.4c
    ],
    ARRAY[
        'provider1',
        'provider1',
        'provider1',
        'provider1'
    ],
    0,            -- start_time
    NULL          -- disabled_at
);

-- substream 1.1c: with substreams 1.1.1p, 1.1.2p
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1.1c',        -- stream_id (1.1c)
    ARRAY[
        '1.1.1p',  -- 1.1.1p
        '1.1.2p'   -- 1.1.2p
    ],
    ARRAY[
        'provider1',
        'provider1'
    ],
    0,            -- start_time
    NULL          -- disabled_at
);

-- substream 1.2c: with substream 1.2.1p
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1.2c',        -- stream_id (1.2c)
    ARRAY[
        '1.2.1p'   -- 1.2.1p
    ],
    ARRAY[
        'provider1'
    ],
    0,            -- start_time
    NULL          -- disabled_at
);

-- start time 5:
-- stream 1c: with substream 1.1c
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1c',          -- stream_id (1c)
    ARRAY[
        '1.1c'     -- 1.1c
    ],
    ARRAY[
        'provider1'
    ],
    5,            -- start_time
    NULL          -- disabled_at
);

-- substream 1.1c: with substream 1.1.1p
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1.1c',        -- stream_id (1.1c)
    ARRAY[
        '1.1.1p'   -- 1.1.1p
    ],
    ARRAY[
        'provider1'
    ],
    5,            -- start_time
    NULL          -- disabled_at
);

-- start time 10:
-- stream 1c: with substream 1.5p
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1c',          -- stream_id (1c)
    ARRAY[
        '1.5p',     -- 1.5p
        '1.6c'     -- 1.6c
    ],
    ARRAY[
        'provider1',
        'provider1'
    ],
    10,           -- start_time
    NULL          -- disabled_at
);

-- start time 6 but disabled:
-- stream 1c: with substream 1.1c
SELECT create_taxonomy(
    'provider1',   -- data_provider
    '1c',          -- stream_id (1c)
    ARRAY[
        '1.1c'     -- 1.1c
    ],
    ARRAY[
        'provider1'
    ],
    6,            -- start_time
    6             -- disabled_at (setting it to the same time as start_time to indicate it's disabled from the beginning)
);
