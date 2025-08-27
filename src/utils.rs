use std::path::PathBuf;

use alloy::{
    primitives::{Address, Bytes},
    providers::{IpcConnect, Provider, ProviderBuilder, WsConnect},
    transports::http::reqwest::Url,
};
use eyre::{Context, bail};
use md5::{Digest, Md5};

pub async fn new_provider(url: Url) -> eyre::Result<Box<dyn Provider>> {
    let p = if url.scheme() == "http" || url.scheme() == "https" {
        Box::new(ProviderBuilder::new().connect_http(url))
    } else if url.scheme() == "ws" || url.scheme() == "wss" {
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url))
            .await
            .context("Failed to connect to websocket")?;

        Box::new(provider)
    } else if url.scheme() == "file" {
        let provider = ProviderBuilder::new()
            .connect_ipc(IpcConnect::new(url.to_string()))
            .await
            .context("Failed to connect to ipc")?;

        Box::new(provider)
    } else {
        bail!("Unsupported scheme: {}", url.scheme());
    };

    Ok(p)
}

pub fn is_empty_dir(path: &PathBuf) -> bool {
    std::fs::read_dir(path)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(false)
}

pub fn temp_dir_path(
    base: PathBuf,
    chain: u64,
    to_block: u64,
    from_block: u64,
    chunk_size: u64,
    tokens: &[Address],
) -> PathBuf {
    let mut hasher = Md5::new();
    tokens.iter().for_each(|t| hasher.update(t));
    let hash: Bytes = hasher.finalize().to_vec().into();

    base.join(format!(
        "{chain}-{to_block}-{from_block}-{chunk_size}-{hash}"
    ))
}

pub fn chunk_inclusive_range(from: u64, to: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::with_capacity((to - from + 1) as usize / chunk_size as usize);
    let mut start = from;

    while start <= to {
        let end = (start + chunk_size - 1).min(to);
        chunks.push((start, end));
        start += chunk_size;
    }

    chunks
}
