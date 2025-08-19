-- Digest config initialization (table + default row)

-- Create single-row config table
CREATE TABLE IF NOT EXISTS digest_config (
    id INT8 PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled BOOL NOT NULL DEFAULT false,
    digest_schedule TEXT NOT NULL,
    updated_at_height INT8 NOT NULL DEFAULT 0
);


