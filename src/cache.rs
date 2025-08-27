use std::{
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use alloy::primitives::{Address, U256};
use arrow::array::{
    Array, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, RecordBatch,
    UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use eyre::{Context as _, ensure};
use futures::StreamExt;
use parquet::{
    arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::fs::{self, File};

use crate::TransferEvent;

const EXTENSION: &str = ".parquet";
pub const FOLDER: &str = "cache";

static SCHEMA: std::sync::LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("token", DataType::FixedSizeBinary(20), false),
        Field::new("from", DataType::FixedSizeBinary(20), false),
        Field::new("to", DataType::FixedSizeBinary(20), false),
        Field::new("value", DataType::Binary, false),
    ]))
});

pub struct CacheWriter {
    writer: AsyncArrowWriter<File>,

    block_number: UInt64Builder,
    log_index: UInt64Builder,
    token: FixedSizeBinaryBuilder,
    from: FixedSizeBinaryBuilder,
    to: FixedSizeBinaryBuilder,
    value: BinaryBuilder,

    cached_rows: usize,
}

impl CacheWriter {
    pub async fn new(path: PathBuf, from: u64, to: u64) -> Result<Self, CacheWriterError> {
        let file = File::create(path.join(filename(from, to)))
            .await
            .map_err(|err| CacheWriterError::Io {
                context: "creating cache file",
                inner: err,
            })?;
        let compression = Compression::ZSTD(ZstdLevel::try_new(5).unwrap());

        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(128 * 1024) // rows per row group (tune as needed)
            .build();

        let writer =
            AsyncArrowWriter::try_new(file, SCHEMA.clone(), Some(props)).map_err(|err| {
                CacheWriterError::Parquet {
                    context: "creating parquet writer",
                    inner: err,
                }
            })?;

        Ok(Self {
            writer,
            block_number: UInt64Builder::new(),
            log_index: UInt64Builder::new(),
            token: FixedSizeBinaryBuilder::new(20),
            from: FixedSizeBinaryBuilder::new(20),
            to: FixedSizeBinaryBuilder::new(20),
            value: BinaryBuilder::new(),
            cached_rows: 0,
        })
    }

    pub async fn write(&mut self, event: &TransferEvent) -> Result<(), CacheWriterError> {
        self.block_number.append_value(event.block);
        self.log_index.append_value(event.log_index);
        self.token.append_value(event.token).unwrap();
        self.from.append_value(event.from).unwrap();
        self.to.append_value(event.to).unwrap();
        self.value.append_value(event.value.as_le_slice());
        self.cached_rows += 1;

        if self.cached_rows >= 10_000 {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn write_batch(&mut self, events: &[TransferEvent]) -> Result<(), CacheWriterError> {
        for event in events {
            self.block_number.append_value(event.block);
            self.log_index.append_value(event.log_index);
            self.token.append_value(event.token).unwrap();
            self.from.append_value(event.from).unwrap();
            self.to.append_value(event.to).unwrap();
            self.value.append_value(event.value.as_le_slice());
            self.cached_rows += events.len();
        }

        if self.cached_rows >= 10_000 {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn finalize(mut self) -> Result<(), CacheWriterError> {
        if self.cached_rows > 0 {
            self.flush().await?;
        }

        self.writer
            .close()
            .await
            .map_err(|err| CacheWriterError::Parquet {
                context: "closing parquet writer",
                inner: err,
            })?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), CacheWriterError> {
        let block_number = self.block_number.finish();
        let log_index = self.log_index.finish();
        let token = self.token.finish();
        let from = self.from.finish();
        let to = self.to.finish();
        let value = self.value.finish();
        self.cached_rows = 0;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(block_number),
                Arc::new(log_index),
                Arc::new(token),
                Arc::new(from),
                Arc::new(to),
                Arc::new(value),
            ],
        )
        .map_err(|err| CacheWriterError::Arrow {
            context: "creating record batch",
            inner: err,
        })?;

        self.writer
            .write(&batch)
            .await
            .map_err(|err| CacheWriterError::Parquet {
                context: "writing record batch",
                inner: err,
            })?;

        Ok(())
    }
}

pub fn filename(from: u64, to: u64) -> String {
    format!("{from}_{to}{EXTENSION}")
}

pub async fn parse_cache_folder(dir: PathBuf) -> eyre::Result<Vec<(u64, u64)>> {
    let mut out = Vec::new();
    let mut rd = fs::read_dir(&dir).await.context("reading cache dir")?;

    while let Some(entry) = rd.next_entry().await.context("Iterating cache dir")? {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(r) = file_name_to_range(&file_name) else {
            continue;
        };

        out.push(r);
    }

    Ok(out)
}

pub async fn read_cache_file(path: &Path) -> eyre::Result<Vec<TransferEvent>> {
    let f = File::open(&path).await.context("opening cache file")?;
    let mut sb = ParquetRecordBatchStreamBuilder::new(f)
        .await
        .context("create stream builder")?
        .build()
        .context("building parquet stream")?;

    let mut results = Vec::new();

    while let Some(batch) = sb.next().await {
        let batch = batch.context("reading batch")?;
        validate_schema(batch.schema().as_ref()).context("validating batch schema")?;

        let (_, columns, rows) = batch.into_parts();
        let mut columns = columns.into_iter();

        macro_rules! col {
            ($t:ty) => {
                cast_array::<$t>(columns.next().unwrap())
            };
        }

        let col_block_number = col!(UInt64Array);
        let col_log_index = col!(UInt64Array);
        let col_token = col!(FixedSizeBinaryArray);
        let col_from = col!(FixedSizeBinaryArray);
        let col_to = col!(FixedSizeBinaryArray);
        let col_value = col!(BinaryArray);

        for i in 0..rows {
            results.push(TransferEvent {
                block: col_block_number.value(i),
                log_index: col_log_index.value(i),
                token: Address::from_slice(col_token.value(i)),
                from: Address::from_slice(col_from.value(i)),
                to: Address::from_slice(col_to.value(i)),
                value: U256::from_le_slice(col_value.value(i)),
            });
        }
    }

    Ok(results)
}

fn cast_array<T: 'static + Array>(array: Arc<dyn Array>) -> Arc<T> {
    unsafe {
        let raw: *const dyn Array = Arc::into_raw(array);
        let raw_t: *const T = raw as *const T;
        Arc::from_raw(raw_t)
    }
}

fn file_name_to_range(n: &str) -> Option<(u64, u64)> {
    let n = n.strip_suffix(EXTENSION)?;
    let mut parts = n.splitn(2, '_');
    let (from_s, to_s) = (parts.next()?, parts.next()?);
    let from = from_s.parse().ok()?;
    let to = to_s.parse().ok()?;
    Some((from, to))
}

#[derive(thiserror::Error, Debug)]
pub enum CacheWriterError {
    #[error("IO error when {context}: {inner}")]
    Io {
        context: &'static str,
        inner: std::io::Error,
    },

    #[error("Parquet error when {context}: {inner}")]
    Parquet {
        context: &'static str,
        inner: parquet::errors::ParquetError,
    },

    #[error("Arrow error when {context}: {inner}")]
    Arrow {
        context: &'static str,
        inner: arrow::error::ArrowError,
    },
}

fn validate_schema(schema: &Schema) -> eyre::Result<()> {
    ensure!(
        schema.fields().len() == SCHEMA.fields().len(),
        "Column count mismatch"
    );

    for (i, (af, ef)) in schema.fields().iter().zip(SCHEMA.fields()).enumerate() {
        ensure!(
            af.name() == ef.name() && af.data_type() == ef.data_type(),
            "Schema mismatch at column {i}: expected `{}` {:?}, got `{}` {:?}",
            ef.name(),
            ef.data_type(),
            af.name(),
            af.data_type()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_file_name_to_range() {
        use super::file_name_to_range;

        assert_eq!(file_name_to_range("100_200.parquet"), Some((100, 200)));
        assert_eq!(file_name_to_range("0_0.parquet"), Some((0, 0)));
        assert_eq!(file_name_to_range("abc_200.parquet"), None);
        assert_eq!(file_name_to_range("100_abc.parquet"), None);
        assert_eq!(file_name_to_range("100_200.txt"), None);
        assert_eq!(file_name_to_range("100200.parquet"), None);
    }
}
