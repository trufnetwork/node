# Prediction Market Indexer API

## Overview

The TRUF.NETWORK Prediction Market Indexer syncs orderbook entities from TN nodes into a centralized PostgreSQL database, providing historical data via a REST API. This is necessary because TN nodes only return **current state** — positions, orders, and rewards are deleted after market settlement.

The indexer captures and preserves:
- Market lifecycle (creation → trading → settlement)
- Order book snapshots at regular intervals (for price charts)
- Per-participant settlement outcomes (wins, losses, payouts)
- LP liquidity reward distributions

## API Endpoints

**Base URLs:**
- Production: `https://indexer.infra.truf.network`
- Testnet: `http://ec2-52-15-66-172.us-east-2.compute.amazonaws.com:8080`

**Swagger UI:** Append `/v0/swagger` to the base URL for interactive documentation.

---

### 1. List Historical Markets

```
GET /v0/prediction-market/markets
```

Returns a list of prediction markets with their settlement results.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | string | `all` | Filter: `active`, `settled`, or `all` |
| `creator` | string | — | Filter by creator wallet address (hex, no 0x prefix) |
| `limit` | int | 50 | Max results (max 100) |
| `cursor` | int | — | Pagination cursor (`created_at` value from previous response) |

**Response:**

```json
{
  "ok": true,
  "meta_data": { "cursor": "49509" },
  "data": [
    {
      "query_id": 34,
      "query_hash": "46a037fda414745163d35055377066a47fce16b764d6eca0a692c4a7f0196109",
      "settle_time": 1770353509,
      "settled": true,
      "winning_outcome": true,
      "settled_at": 1770354000,
      "created_at": 49509,
      "creator": "32a46917df74808b9add7dc6ef0c34520412fdf3",
      "max_spread": 10,
      "min_order_size": "1000000000000000000",
      "bridge": "hoodi_tt2"
    }
  ]
}
```

**Field Descriptions:**

| Field | Description |
|-------|-------------|
| `query_id` | Unique market identifier (from `ob_queries.id` on-chain) |
| `query_hash` | Deterministic hash of market parameters |
| `settle_time` | Unix timestamp when market is scheduled to settle |
| `settled` | Whether the market has been settled |
| `winning_outcome` | `true` = YES won, `false` = NO won, `null` = not settled |
| `settled_at` | Unix timestamp when settlement occurred |
| `created_at` | Block height when the market was created |
| `creator` | Wallet address of market creator (hex, lowercase) |
| `max_spread` | Maximum bid-ask spread for LP reward eligibility (in cents) |
| `min_order_size` | Minimum order size in wei (18 decimals) |
| `bridge` | Collateral bridge used (e.g., `hoodi_tt2` for USDC) |

---

### 2. Market Order Book Snapshots

```
GET /v0/prediction-market/markets/{query_id}/snapshots
```

Returns order book state at intervals — used to build price charts.

**Path Parameters:**

| Parameter | Description |
|-----------|-------------|
| `query_id` | Market ID |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 50 | Max snapshots returned |
| `cursor` | int | — | Pagination cursor (block height) |

**Response:**

```json
{
  "ok": true,
  "data": [
    {
      "query_id": 34,
      "block_height": 49520,
      "timestamp": 1770352800,
      "yes_bid_price": 55,
      "yes_ask_price": 60,
      "no_bid_price": 40,
      "no_ask_price": 45,
      "yes_volume": 150,
      "no_volume": 120,
      "midpoint_price": 57,
      "spread": 5
    }
  ]
}
```

**Field Descriptions:**

| Field | Description |
|-------|-------------|
| `block_height` | Block at which the snapshot was taken |
| `timestamp` | Unix timestamp of the block |
| `yes_bid_price` | Best YES buy price (1-99 cents) |
| `yes_ask_price` | Best YES sell price (1-99 cents) |
| `no_bid_price` | Best NO buy price (1-99 cents) |
| `no_ask_price` | Best NO sell price (1-99 cents) |
| `yes_volume` | Total YES shares in the order book |
| `no_volume` | Total NO shares in the order book |
| `midpoint_price` | `(yes_ask + yes_bid) / 2` |
| `spread` | `yes_ask - yes_bid` |

---

### 3. Participant Settlement History

```
GET /v0/prediction-market/participants/{wallet_address}/settlements
```

Returns historical settlement results for a specific participant across all markets.

**Path Parameters:**

| Parameter | Description |
|-----------|-------------|
| `wallet_address` | Wallet address (hex, no 0x prefix) |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 50 | Max results |
| `cursor` | int | — | Pagination cursor (timestamp) |

**Response:**

```json
{
  "ok": true,
  "data": {
    "wallet_address": "1c6790935a3a1A6B914399Ba743BEC8C41Fe89Fb",
    "settlements": [
      {
        "query_id": 34,
        "winning_shares": 8,
        "losing_shares": 0,
        "payout": "7840000000000000000",
        "refunded_collateral": "3000000000000000000",
        "timestamp": 1770354000
      }
    ],
    "total_won": 2,
    "total_lost": 0
  }
}
```

**Field Descriptions:**

| Field | Description |
|-------|-------------|
| `winning_shares` | Number of shares on the winning side |
| `losing_shares` | Number of shares on the losing side |
| `payout` | Amount received from winning shares, in wei. Winners receive $0.98 per share (2% redemption fee goes to LP pool) |
| `refunded_collateral` | Collateral returned from unmatched buy orders, in wei |
| `total_won` | Number of markets where `winning_shares > losing_shares` |
| `total_lost` | Number of markets where `losing_shares > winning_shares` |

**Note on settlement model:** The protocol uses a **2% redemption fee**. Winners receive $0.98 per winning share (not $1.00). The 2% fee is distributed to liquidity providers.

---

### 4. Participant LP Rewards

```
GET /v0/prediction-market/participants/{wallet_address}/rewards
```

Returns historical liquidity provider reward distributions for a specific participant.

**Path Parameters:**

| Parameter | Description |
|-----------|-------------|
| `wallet_address` | Wallet address (hex, no 0x prefix) |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 50 | Max results |
| `cursor` | int | — | Pagination cursor (distributed_at timestamp) |

**Response:**

```json
{
  "ok": true,
  "data": {
    "wallet_address": "c11Ff6d3cC60823EcDCAB1089F1A4336053851EF",
    "rewards": [
      {
        "query_id": 34,
        "total_reward_percent": 29.41,
        "reward_amount": "2698000000000000000",
        "blocks_sampled": 1,
        "distributed_at": 1770354000
      }
    ],
    "total_rewards": "22283000000000000000"
  }
}
```

**Field Descriptions:**

| Field | Description |
|-------|-------------|
| `total_reward_percent` | Percentage of the fee pool this LP received (0-100) |
| `reward_amount` | Amount received in wei (18 decimals) |
| `blocks_sampled` | Number of blocks where LP positions were sampled |
| `distributed_at` | Unix timestamp when rewards were distributed |
| `total_rewards` | Sum of all reward amounts across all markets, in wei |

**Note on LP rewards:** Rewards come from two sources: (1) trading fees from bid-ask spread, and (2) the 2% redemption fee from settlement. Rewards are distributed proportionally based on liquidity provision measured across sampled blocks.

---

## Common Patterns

### Pagination

All list endpoints support cursor-based pagination. When there are more results, the response includes a `cursor` in `meta_data`:

```json
{
  "meta_data": { "cursor": "49509" }
}
```

Pass this value back as the `cursor` query parameter to get the next page.

### Wei to Token Conversion

All monetary values (`payout`, `refunded_collateral`, `reward_amount`, `min_order_size`) are in **wei** (18 decimal places). To convert to human-readable token amounts:

```
1000000000000000000 wei = 1.00 USDC
7840000000000000000 wei = 7.84 USDC
```

### Wallet Address Format

Wallet addresses in the API use **hex format without the 0x prefix**:
- Correct: `1c6790935a3a1A6B914399Ba743BEC8C41Fe89Fb`
- Incorrect: `0x1c6790935a3a1A6B914399Ba743BEC8C41Fe89Fb`

---

## SDK Examples

Working examples showing how to query all 4 endpoints using standard HTTP clients:

- **Python**: [`sdk-py/examples/indexer/query_indexer.py`](https://github.com/trufnetwork/sdk-py/tree/main/examples/indexer)
- **Go**: [`sdk-go/examples/indexer/main.go`](https://github.com/trufnetwork/sdk-go/tree/main/examples/indexer)
- **TypeScript**: [`sdk-js/examples/indexer/query_indexer.ts`](https://github.com/trufnetwork/sdk-js/tree/main/examples/indexer)

No SDK wrapper is required — the indexer API is a standard REST/JSON API queryable with any HTTP client.

---

## Architecture

```
TN Node (current state) ──polling──> Indexer (historical state) ──REST API──> Clients
     │                                    │
     ├── ob_queries (markets)             ├── pm_markets
     ├── ob_positions (orders)            ├── pm_market_snapshots
     ├── ob_rewards (LP data)             ├── pm_participant_settlements
     └── (deleted after settlement)       ├── pm_liquidity_rewards
                                          └── pm_positions (temporary, until settlement)
```

The indexer polls the TN node every 30 seconds to sync market data, order book snapshots, and positions. When a market settles, the indexer calculates settlement outcomes from tracked positions (since the node deletes positions after settlement) and stores the results permanently.

## Source Code

- **Indexer**: [`github.com/trufnetwork/indexer`](https://github.com/trufnetwork/indexer)
- **API routes**: `indexer/api/pm_routes.go`
- **Sync logic**: `indexer/cmd/pm_sync.go`
- **Schema**: `indexer/store/schema.sql`
