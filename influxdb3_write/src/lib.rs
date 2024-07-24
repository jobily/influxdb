//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the open
//! segment in the buffer. If the buffer has a WAL configured, the data is written into the WAL before being
//! buffered in memory.
//!
//! When the segment reaches a certain size, or a certain amount of time has passed, it will be closed and marked
//! to be persisted. A new open segment will be created and new writes will be written to that segment.

pub mod cache;
pub mod catalog;
mod chunk;
mod last_cache;
pub mod paths;
pub mod persister;
pub mod write_buffer;

use crate::paths::{ParquetFilePath, SnapshotInfoFilePath};
use async_trait::async_trait;
use bytes::Bytes;
use data_types::{NamespaceName, Timestamp, TimestampMinMax};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::Expr;
use iox_query::QueryChunk;
use iox_time::Time;
use last_cache::LastCacheProvider;
use parquet::format::FileMetaData;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use influxdb3_wal::WalFileSequenceNumber;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database not found {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("object store path error: {0}")]
    ObjStorePath(#[from] object_store::path::Error),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),

    #[error("persister error: {0}")]
    Persister(#[from] persister::Error),

    #[error("invalid segment duration {0}. Must be one of 1m, 5m, 10m, 15m, 30m, 1h, 2h, 4h")]
    InvalidSegmentDuration(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait WriteBuffer: Bufferer + ChunkContainer {}

/// The buffer is for buffering data in memory before it is persisted to object storage. The buffer is queryable and
/// aims to use as little memory as possible, converting data into in-memory Parquet data periodically as it arrives
/// and is queried out. The buffer is responsible for keeping a consistent view of the catalog and  is also responsible
/// for writing data into the WAL if it is configured.
///
/// Data in the buffer is organized into monotonically increasing segments, each associated with a single WAL file, if
/// the WAL is configured. As a segment is closed, it is persisted and then freed from memory.
#[async_trait]
pub trait Bufferer: Debug + Send + Sync + 'static {
    /// Validates the line protocol, writes it into the WAL if configured, writes it into the in memory buffer
    /// and returns the result with any lines that had errors and summary statistics. This writes into the currently
    /// open segment or it will open one. The open segment id and the memory usage of the currently open segment are
    /// returned.
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Write v3 line protocol
    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Returns the catalog
    fn catalog(&self) -> Arc<catalog::Catalog>;

    fn last_cache(&self) -> Arc<LastCacheProvider>;
}

/// ChunkContainer is used by the query engine to get chunks for a given table. Chunks will generally be in the
/// `Bufferer` for those in memory from buffered writes or the `Persister` for parquet files that have been persisted
/// from previously buffered segments.
pub trait ChunkContainer: Debug + Send + Sync + 'static {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

const RANGE_KEY_TIME_FORMAT: &str = "%Y-%m-%dT%H-%M";

/// The duration of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level0Duration(Duration);

impl Level0Duration {
    pub fn duration_seconds(&self) -> i64 {
        self.0.as_secs() as i64
    }

    /// Returns the time of the chunk the given timestamp belongs to based on the duration
    pub fn chunk_time_for_timestamp(&self, t: Timestamp) -> i64 {
        t.get() % self.0.as_nanos() as i64
    }

    /// Given a time, returns the start time of the segment that contains the time.
    pub fn start_time(&self, timestamp_seconds: i64) -> Time {
        let duration_seconds = self.duration_seconds();
        let rounded_seconds = (timestamp_seconds / duration_seconds) * duration_seconds;
        Time::from_timestamp(rounded_seconds, 0).unwrap()
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }

    pub fn new_5m() -> Self {
        Self(Duration::from_secs(300))
    }
}

impl FromStr for Level0Duration {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "1m" => Ok(Self(Duration::from_secs(60))),
            "5m" => Ok(Self(Duration::from_secs(300))),
            "10m" => Ok(Self(Duration::from_secs(600))),
            "15m" => Ok(Self(Duration::from_secs(900))),
            "30m" => Ok(Self(Duration::from_secs(1800))),
            "1h" => Ok(Self(Duration::from_secs(3600))),
            "2h" => Ok(Self(Duration::from_secs(7200))),
            "4h" => Ok(Self(Duration::from_secs(14400))),
            _ => Err(Error::InvalidSegmentDuration(s.to_string())),
        }
    }
}

pub const DEFAULT_OBJECT_STORE_URL: &str = "iox://influxdb3/";

#[async_trait]
pub trait Persister: Debug + Send + Sync + 'static {
    type Error;

    /// Loads the most recently persisted catalog from object storage.
    async fn load_catalog(&self) -> Result<Option<PersistedCatalog>, Self::Error>;

    /// Loads the most recently persisted N snapshot parquet file lists from object storage.
    async fn load_snapshots(
        &self,
        most_recent_n: usize,
    ) -> Result<Vec<PersistedSnapshot>, Self::Error>;

    // Loads a Parquet file from ObjectStore
    async fn load_parquet_file(&self, path: ParquetFilePath) -> Result<Bytes, Self::Error>;

    /// Persists the catalog with the given segment ID. If this is the highest segment ID, it will
    /// be the catalog that is returned the next time `load_catalog` is called.
    async fn persist_catalog(
        &self,
        wal_file_sequence_number: WalFileSequenceNumber,
        catalog: catalog::Catalog,
    ) -> Result<(), Self::Error>;

    /// Persists the snapshot file
    async fn persist_snapshot(&self, persisted_snapshot: &PersistedSnapshot) -> crate::persister::Result<()>;

    // Writes a SendableRecorgBatchStream to the Parquet format and persists it
    // to Object Store at the given path. Returns the number of bytes written and the file metadata.
    async fn persist_parquet_file(
        &self,
        path: ParquetFilePath,
        record_batch: SendableRecordBatchStream,
    ) -> Result<(u64, FileMetaData), Self::Error>;

    /// Returns the configured `ObjectStore` that data is loaded from and persisted to.
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore>;

    // This is used by the query engine to know where to read parquet files from. This assumes
    // that there is a `ParquetStorage` with an id of `influxdb3` and that this url has been
    // registered with the query execution context. Kind of ugly here, but not sure where else
    // to keep this.
    fn object_store_url(&self) -> ObjectStoreUrl {
        ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap()
    }

    fn as_any(&self) -> &dyn Any;
}

/// A single write request can have many lines in it. A writer can request to accept all lines that are valid, while
/// returning an error for any invalid lines. This is the error information for a single invalid line.
#[derive(Debug, Serialize)]
pub struct WriteLineError {
    pub original_line: String,
    pub line_number: usize,
    pub error_message: String,
}

/// A write that has been validated against the catalog schema, written to the WAL (if configured), and buffered in
/// memory. This is the summary information for the write along with any errors that were encountered.
#[derive(Debug)]
pub struct BufferedWriteRequest {
    pub db_name: NamespaceName<'static>,
    pub invalid_lines: Vec<WriteLineError>,
    pub line_count: usize,
    pub field_count: usize,
    pub index_count: usize,
}

/// A persisted Catalog that contains the database, table, and column schemas.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PersistedCatalog {
    /// The wal file number that triggered the snapshot to persisst this catalog
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The catalog that was persisted.
    pub catalog: catalog::InnerCatalog,
}

/// The collection of Parquet files that were persisted in a snapshot
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PersistedSnapshot {
    /// The wal file sequence number that triggered this snapshot
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The size of the segment parquet files in bytes.
    pub parquet_size_bytes: u64,
    /// The number of rows across all parquet files in the segment.
    pub row_count: u64,
    /// The min time from all parquet files in the segment.
    pub min_time: i64,
    /// The max time from all parquet files in the segment.
    pub max_time: i64,
    /// The collection of databases that had tables persisted in this segment. The tables will then have their
    /// name and the parquet files.
    pub databases: HashMap<String, DatabaseTables>,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseTables {
    pub tables: hashbrown::HashMap<String, TableParquetFiles>,
}

/// A collection of parquet files persisted in a segment for a specific table.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TableParquetFiles {
    /// The table name.
    pub table_name: String,
    /// The parquet files persisted in this segment for this table.
    pub parquet_files: Vec<ParquetFile>,
    /// The sort key used for all parquet files in this segment.
    pub sort_key: Vec<String>,
}

/// The summary data for a persisted parquet file in a segment.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ParquetFile {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    pub min_time: i64,
    pub max_time: i64,
}

impl ParquetFile {
    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.min_time,
            max: self.max_time,
        }
    }
}

/// The precision of the timestamp
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Auto,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Auto
    }
}

impl From<iox_http::write::Precision> for Precision {
    fn from(legacy: iox_http::write::Precision) -> Self {
        match legacy {
            iox_http::write::Precision::Second => Precision::Second,
            iox_http::write::Precision::Millisecond => Precision::Millisecond,
            iox_http::write::Precision::Microsecond => Precision::Microsecond,
            iox_http::write::Precision::Nanosecond => Precision::Nanosecond,
        }
    }
}

/// Guess precision based off of a given timestamp.
// Note that this will fail in June 2128, but that's not our problem
pub(crate) fn guess_precision(timestamp: i64) -> Precision {
    const NANO_SECS_PER_SEC: i64 = 1_000_000_000;
    // Get the absolute value of the timestamp so we can work with negative
    // numbers
    let val = timestamp.abs() / NANO_SECS_PER_SEC;

    if val < 5 {
        // If the time sent to us is in seconds then this will be a number less than
        // 5 so for example if the time in seconds is 1_708_976_567 then it will be
        // 1 (due to integer truncation) and be less than 5
        Precision::Second
    } else if val < 5_000 {
        // If however the value is milliseconds and not seconds than the same number
        // for time but now in milliseconds 1_708_976_567_000 when divided will now
        // be 1708 which is bigger than the previous if statement but less than this
        // one and so we return milliseconds
        Precision::Millisecond
    } else if val < 5_000_000 {
        // If we do the same thing here by going up another order of magnitude then
        // 1_708_976_567_000_000 when divided will be 1708976 which is large enough
        // for this if statement
        Precision::Microsecond
    } else {
        // Anything else we can assume is large enough of a number that it must
        // be nanoseconds
        Precision::Nanosecond
    }
}

#[cfg(test)]
mod test_helpers {
    use crate::catalog::Catalog;
    use influxdb3_wal::{TableChunk, WriteBatch};
    use crate::write_buffer::validator::WriteValidator;
    use crate::{Precision, Level0Duration};
    use data_types::NamespaceName;
    use iox_time::Time;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(crate) fn lp_to_write_batch(
        catalog: Arc<Catalog>,
        db_name: &'static str,
        lp: &str,
    ) -> WriteBatch {
        let db_name = NamespaceName::new(db_name).unwrap();
        let result = WriteValidator::initialize(db_name.clone(), catalog)
            .unwrap()
            .v1_parse_lines_and_update_schema(lp, false)
            .unwrap()
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                Level0Duration::new_5m(),
                Precision::Nanosecond,
            );

        result.valid_data
    }
}

#[cfg(test)]
pub(crate) mod test_help {
    use iox_query::exec::DedicatedExecutor;
    use iox_query::exec::Executor;
    use iox_query::exec::ExecutorConfig;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use parquet_file::storage::ParquetStorage;
    use parquet_file::storage::StorageId;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    pub(crate) fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            },
            DedicatedExecutor::new_testing(),
        ))
    }
}
