# ERC20 Token Holders Analyzer

A high-performance Rust tool for analyzing ERC20 token holders by crawling transfer events from Ethereum nodes.

## Overview

This tool helps you analyze ERC20 token holder data by:

1. Fetching ERC20 `Transfer` events from an Ethereum node
2. Processing and calculating token balances for each address
3. Caching intermediate results for efficient resume capability
4. Generating CSV reports of token holders and their balances

## Features

- **High Performance**: Parallel processing of blockchain data with configurable concurrency
- **Resume Capability**: Caches downloaded data to seamlessly resume interrupted jobs
- **Adaptive Chunking**: Automatically adjusts request sizes when nodes limit response size
- **Multiple Token Support**: Analyze multiple token contracts in a single run
- **Support for Different RPC Types**: HTTP, WebSocket, and IPC connections

## Usage

Basic example:

```bash
./target/release/erc20-holders \
    --rpc https://eth.merkle.io \
    --chunk-size 5000 \
    --concurrent-requests 10 \
    0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E \
    0xD33526068D116cE69F19A9ee46F0bd304F21A51f \
    <MORE TOKENS...>
```


### Examples

Analyze DAI token holders from blocks 15,000,000 to 16,000,000:

```bash
erc20-holders \
  --rpc https://eth.merkle.io \
  --from-block 15000000 \
  --to-block 16000000 \
  0x6b175474e89094c44da98b954eedeac495271d0f
```

Analyze multiple tokens with increased parallelism:

```bash
erc20-holders \
  --rpc https://eth.merkle.io \
  --concurrent-requests 20 \
  0x6b175474e89094c44da98b954eedeac495271d0f 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 \
```

Use IPC connection:

```bash
erc20-holders \
  --rpc file:///tmp/reth.ipc \
  0x6b175474e89094c44da98b954eedeac495271d0f
```

## Output

The tool generates CSV files in the output directory, one file per token contract. Each file contains:

- `account`: The address of the token holder
- `balance`: The token balance (can be positive or negative)

## Cache System

Downloaded logs are cached in the `data/cache/` directory as Parquet files. This enables efficient resuming of interrupted jobs. Use `--no-resume` to disable this feature.
