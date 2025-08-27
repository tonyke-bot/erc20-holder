pub mod balance;
pub mod cache;
pub mod utils;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use alloy::{
    primitives::{Address, U256},
    providers::Provider,
    rpc::{
        client::BatchRequest,
        types::{Filter, Log},
    },
    sol,
    sol_types::SolEvent,
    transports::{RpcError, TransportErrorKind, http::reqwest::Url},
};
use clap::Parser;
use futures::{StreamExt as _, stream};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::{fs, signal::ctrl_c, sync::Semaphore};

use crate::{
    balance::BalanceSheet,
    cache::{CacheWriter, read_cache_file},
};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:8545", env = "ETH_RPC_URL")]
    rpc: Url,

    #[arg(value_delimiter = ',')]
    tokens: Vec<Address>,

    #[arg(long, default_value = "0", help = "Starting block number, inclusive")]
    from_block: u64,

    #[arg(long, help = "Ending block number, inclusive. Default to latest")]
    to_block: Option<u64>,

    #[arg(long, default_value = "./data")]
    output: PathBuf,

    #[arg(
        long,
        default_value = "1000",
        help = "Number of blocks to query in each eth_getLogs"
    )]
    chunk_size: usize,

    #[arg(
        long,
        default_value = "10",
        help = "Number of concurrent requests to make"
    )]
    concurrent_requests: usize,

    #[arg(long, help = "Do not cache downloaded logs and resume from cache")]
    no_resume: bool,

    #[arg(long, help = "Enable debug logging")]
    debug_log: bool,

    #[arg(
        long,
        default_value = "30",
        help = "Timeout in seconds for each eth_getLogs"
    )]
    timeout: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let provider = utils::new_provider(args.rpc.clone())
        .await
        .expect("Failed to connect to RPC");

    let chain_id = provider
        .get_chain_id()
        .await
        .expect("Failed to get chain ID");

    if args.tokens.is_empty() {
        eprintln!("Error: at least one --token is required!");
        std::process::exit(1);
    }

    let to_block = match args.to_block {
        Some(b) => b,
        None => provider
            .get_block_number()
            .await
            .expect("Failed to get latest block"),
    };

    if args.from_block > to_block {
        eprintln!(
            "Error: from_block {} is greater than to_block {}!",
            args.from_block, to_block
        );
        std::process::exit(1);
    }

    let mut ranges =
        utils::chunk_inclusive_range(args.from_block, to_block, args.chunk_size as u64)
            .into_iter()
            .rev()
            .collect::<Vec<_>>();

    println!("Chain ID:   {}", chain_id);
    println!("Block:      {}-{}", args.from_block, to_block);
    println!("Tokens:     {:?}", args.tokens);
    println!("Output:     {}", args.output.to_string_lossy());
    println!("Chunks:     {:?}", ranges.len());
    println!("Chunk size: {}", args.chunk_size);
    println!();

    // Ensure output folder exists
    fs::create_dir_all(&args.output)
        .await
        .expect("Failed to create output directory");

    // States
    let pb = ProgressBar::new(ranges.len() as u64).with_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} ETA:{eta} Elapsed:{elapsed}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.tick();

    let mut balances = BalanceSheet::default();
    let mut total_transfers = 0;
    let mut failed_chunk = 0;
    let semaphore = Semaphore::new(args.concurrent_requests);

    // Read cache
    let cache_dir = if args.no_resume {
        None
    } else {
        let p = args.output.join(cache::FOLDER);

        fs::create_dir_all(&p)
            .await
            .expect("Failed to create cache directory");

        pb.println("Reading cache...");
        read_cache(&p, &pb, &mut ranges, &mut balances).await;

        Some(p)
    };

    pb.println("Download and handling events...");

    // Crawl logs with parallelism
    let mut result_stream = create_job_results_stream(
        &semaphore,
        &pb,
        &ranges,
        provider.as_ref(),
        &cache_dir,
        &args,
    );

    // Handle results
    let finish = loop {
        let job_result = tokio::select! {
            biased;
            _ = ctrl_c() => break false,
            r = result_stream.next() => r,
        };

        let Some(Some(job_result)) = job_result else {
            break true;
        };

        let result = match job_result.result {
            Ok(r) => r,
            Err(e) => {
                pb.println(format!(
                    "Error processing job of blocks {}-{}: {:#}",
                    job_result.from, job_result.to, e
                ));
                failed_chunk += 1;
                continue;
            }
        };

        let count = result.len();
        balances.extend(&result);
        total_transfers += count;
        pb.inc(1);
    };

    pb.finish_and_clear();

    println!("Total transfers: {}", total_transfers);

    if !finish {
        println!("Received Ctrl+C, shutting down...");
        semaphore.close();
        if tokio::time::timeout(
            Duration::from_secs(args.timeout),
            semaphore.acquire_many(args.concurrent_requests as u32),
        )
        .await
        .is_err()
        {
            println!("Timeout waiting for jobs to be finished, exiting anyway...");
        }

        return;
    }

    if failed_chunk > 0 {
        println!("Failed chunks: {}", failed_chunk);

        if !args.no_resume {
            println!(
                "There are some missing logs that are failed to be downloaded, please re-run the program with same parameters to finish the job"
            );
        }

        std::process::exit(-1);
    } else {
        balances
            .finalize(&args.output, args.from_block, to_block)
            .await
            .expect("Failed to write balances");

        println!("Done");
    }
}

async fn read_cache(
    cache_dir: &Path,
    pb: &ProgressBar,
    ranges: &mut Vec<(u64, u64)>,
    balances: &mut BalanceSheet,
) {
    let mut used_cache = HashSet::new();

    for &(from, to) in ranges.iter() {
        let path = cache_dir.join(cache::filename(from, to));
        let exists = fs::metadata(&path).await.is_ok_and(|meta| meta.is_file());

        if !exists {
            continue;
        }

        let c = match read_cache_file(&path).await {
            Ok(c) => c,
            Err(e) => {
                pb.println(format!(
                    "Error reading cache file {}, it might be corrupted: {:#}",
                    path.display(),
                    e
                ));
                continue;
            }
        };

        balances.extend(&c);
        used_cache.insert((from, to));
        pb.inc(1);
    }

    ranges.retain(|r| !used_cache.contains(r));
}

fn create_job_results_stream<'a>(
    semaphore: &'a Semaphore,
    pb: &'a ProgressBar,
    ranges: &'a [(u64, u64)],
    provider: &'a dyn Provider,
    cache_dir: &'a Option<PathBuf>,
    args: &'a Args,
) -> impl futures::Stream<Item = Option<JobResult>> + 'a {
    let filter = Filter::default()
        .address(args.tokens.clone())
        .event_signature(Transfer::SIGNATURE_HASH);

    let &Args {
        concurrent_requests,
        timeout,
        debug_log,
        ..
    } = args;

    let timeout = Duration::from_secs(timeout);

    stream::iter(ranges.iter().copied().map(move |(from, to)| {
        let filter = filter.clone().from_block(from).to_block(to);
        let semaphore = semaphore;
        let provider = provider;
        let cache_dir = cache_dir;

        async move {
            let Ok(_p) = semaphore.acquire().await else {
                return None;
            };

            if debug_log {
                pb.println(format!("Fetching logs for blocks {from}-{to}"));
            }

            let f = pull_logs(cache_dir, pb, provider, &filter, from, to);
            let result = tokio::time::timeout(timeout, f)
                .await
                .unwrap_or(Err(DownloadError::Timeout));

            if debug_log {
                pb.println(format!("Finished fetching blocks {from}-{to}"));
            }

            Some(JobResult { from, to, result })
        }
    }))
    .buffer_unordered(concurrent_requests)
}

async fn pull_logs(
    cache_dir: &Option<PathBuf>,
    pb: &ProgressBar,
    provider: &dyn Provider,
    filter: &Filter,
    from: u64,
    to: u64,
) -> Result<Vec<TransferEvent>, DownloadError> {
    let cache = if let Some(cache_dir) = cache_dir {
        let c = CacheWriter::new(cache_dir.clone(), from, to).await?;
        Some(c)
    } else {
        None
    };

    let logs = get_logs_smart(provider, pb, filter, from, to, 10).await?;
    let mut events = Vec::with_capacity(logs.len());

    for (i, log) in logs.into_iter().enumerate() {
        let Some(block) = log.block_number else {
            return Err(DownloadError::MissingBlockNumber { index_in_vec: i });
        };

        let Some(log_index) = log.log_index else {
            return Err(DownloadError::MissingLogIndex { block });
        };

        let Ok(e) = log.log_decode::<Transfer>() else {
            continue;
        };

        let event = TransferEvent {
            block,
            log_index,
            token: e.inner.address,
            from: e.inner.from,
            to: e.inner.to,
            value: e.inner.value,
        };

        events.push(event);
    }

    if let Some(mut cache) = cache {
        cache.write_batch(&events).await?;
        cache.finalize().await?;
    }

    Ok(events)
}

async fn get_logs_smart(
    provider: &dyn Provider,
    pb: &ProgressBar,
    filter: &Filter,
    from: u64,
    to: u64,
    chunk_size_if_needed: usize,
) -> Result<Vec<Log>, DownloadError> {
    let logs = provider
        .get_logs(&filter.clone().from_block(from).to_block(to))
        .await;

    let err = match logs {
        Ok(logs) => return Ok(logs),
        Err(err) => err,
    };

    pb.println(format!(
        "Warning: eth_getLogs for blocks {from}-{to} failed because the chunk is too large, retrying with smaller chunks...",
    ));

    // In the case of results too long, reth returns -32602
    if err.as_error_resp().is_none_or(|resp| resp.code != -32602) {
        return Err(err.into());
    }

    let ranges = utils::chunk_inclusive_range(
        filter.get_from_block().unwrap(),
        filter.get_to_block().unwrap(),
        chunk_size_if_needed as u64,
    );

    let mut batch = BatchRequest::new(provider.client());
    let mut waiters = Vec::with_capacity(ranges.len());

    for &(from, to) in &ranges {
        let filter = filter.clone().from_block(from).to_block(to);
        let waiter = batch
            .add_call::<_, Vec<Log>>("eth_getLogs", &[filter])
            .unwrap();

        waiters.push(waiter);
    }

    batch
        .await
        .map_err(|e| DownloadError::RpcError(Arc::new(e)))?;

    let mut logs = Vec::with_capacity(10000);

    for waiter in waiters {
        logs.extend(waiter.await?);
    }

    Ok(logs)
}

pub struct TransferEvent {
    block: u64,
    log_index: u64,
    token: Address,
    from: Address,
    to: Address,
    value: U256,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("RPC error: {0}")]
    RpcError(Arc<RpcError<TransportErrorKind>>),

    #[error("Log missing block number at index {index_in_vec}")]
    MissingBlockNumber { index_in_vec: usize },

    #[error("Log missing log index for block {block}")]
    MissingLogIndex { block: u64 },

    #[error("Cache error: {0}")]
    CacheError(Arc<cache::CacheWriterError>),

    #[error("Timeout error")]
    Timeout,
}

impl From<RpcError<TransportErrorKind>> for DownloadError {
    fn from(e: RpcError<TransportErrorKind>) -> Self {
        DownloadError::RpcError(Arc::new(e))
    }
}

impl From<cache::CacheWriterError> for DownloadError {
    fn from(e: cache::CacheWriterError) -> Self {
        DownloadError::CacheError(Arc::new(e))
    }
}

pub struct JobResult {
    from: u64,
    to: u64,
    result: Result<Vec<TransferEvent>, DownloadError>,
}
