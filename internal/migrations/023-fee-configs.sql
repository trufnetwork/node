-- Fee configuration table for ERC20 bridge operations
-- Stores configurable fees and treasury address for deposit and withdraw operations

CREATE TABLE IF NOT EXISTS fee_configs (
    operation_type TEXT NOT NULL,
    fee_percentage NUMERIC(4, 4) NOT NULL,
    treasury_address TEXT NOT NULL,
    created_at INT8 NOT NULL,
    updated_at INT8 NOT NULL,

    PRIMARY KEY (operation_type),

    -- Constraints
    CHECK (operation_type IN ('deposit', 'withdraw')),
    CHECK (fee_percentage >= 0 AND fee_percentage <= 1), -- 0% to 100%
    CHECK (treasury_address LIKE '0x%' AND LENGTH(treasury_address) = 42)
);

-- Insert default fee configurations
INSERT INTO fee_configs (operation_type, fee_percentage, treasury_address, created_at, updated_at) VALUES
('deposit', 0.0100, '0xDe5B2aBce299eBdC3567895B1B4b02Ca2c33C94A', @height, @height),
('withdraw', 0.0100, '0xDe5B2aBce299eBdC3567895B1B4b02Ca2c33C94A', @height, @height)
ON CONFLICT (operation_type) DO NOTHING;

