-- we'll use this to test configuration of the erc20 bridge in tests.
-- if we successfully emit events and this listens, then we know the bridge is configured correctly.
USE kwil_ordered_sync as kwil_ordered_sync;
USE kwil_erc20_meta as kwil_erc20_meta;

USE erc20 {
    chain: 'sepolia',
    escrow: '0x1111111111111111111111111111111111111111'
} AS sepolia_bridge;

CREATE ACTION get_balance($wallet_address TEXT) public {
    $balance := sepolia_bridge.balance($wallet_address);
    return $balance;
}