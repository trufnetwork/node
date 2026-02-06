# Testnet Wallet Funding Guide

This guide explains how to get test tokens for the TRUF.NETWORK testnet prediction markets.

## Overview

TRUF.NETWORK testnet uses test tokens deployed on the **Hoodi** Ethereum testnet (a public Ethereum test network). You'll need:
1. Hoodi ETH (for gas fees)
2. Test tokens (TT2 for USDC)

Once you have tokens on Hoodi, use the [TRUF.NETWORK Bridge](https://truf.network/account/bridge?test=true) to deposit them to TRUF.NETWORK testnet.

## Step 1: Get Hoodi ETH (for gas)

Go to [Google Cloud Web3 Faucet](https://cloud.google.com/application/web3/faucet/ethereum/hoodi) and request Hoodi ETH for your wallet address.

## Step 2: Install Foundry

Foundry provides the `cast` command-line tool for interacting with Ethereum contracts.

Follow the installation guide: [https://getfoundry.sh/introduction/installation](https://getfoundry.sh/introduction/installation)

## Step 3: Mint Test Tokens

### Test Token Contracts

| Token | Purpose | Contract Address |
|-------|---------|------------------|
| TT2 (USDC) | Test USDC for prediction markets | `0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd` |
| TT (TRUF) | Test TRUF token | `0x263ce78fef26600e4e428cebc91c2a52484b4fbf` |

### Mint TT2 (Test USDC)

```bash
cast send 0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd \
  "mint(uint256)" 1000000000000000000000 \
  --rpc-url wss://0xrpc.io/hoodi \
  --private-key YOUR_PRIVATE_KEY
```

This mints 1000 TT2 tokens. Adjust the amount as needed:

| Amount | Wei Value |
|--------|-----------|
| 100 tokens | `100000000000000000000` |
| 1,000 tokens | `1000000000000000000000` |
| 10,000 tokens | `10000000000000000000000` |

### Mint TT (Test TRUF)

```bash
cast send 0x263ce78fef26600e4e428cebc91c2a52484b4fbf \
  "mint(uint256)" 1000000000000000000000 \
  --rpc-url wss://0xrpc.io/hoodi \
  --private-key YOUR_PRIVATE_KEY
```

## Step 4: Deposit to TRUF.NETWORK

After minting tokens on Hoodi, deposit them to TRUF.NETWORK using the web interface:

**[https://truf.network/account/bridge?test=true](https://truf.network/account/bridge?test=true)**

Connect your wallet and follow the deposit flow.

## Quick Reference

```bash
# Mint 1000 TT2 (USDC)
cast send 0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd "mint(uint256)" 1000000000000000000000 --rpc-url wss://0xrpc.io/hoodi --private-key YOUR_PRIVATE_KEY

# Mint 1000 TT (TRUF)
cast send 0x263ce78fef26600e4e428cebc91c2a52484b4fbf "mint(uint256)" 1000000000000000000000 --rpc-url wss://0xrpc.io/hoodi --private-key YOUR_PRIVATE_KEY
```
