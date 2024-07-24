//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub mod persisted_files;
mod table_buffer;
pub(crate) mod validator;
mod queryable_buffer;

use crate::cache::ParquetCache;
use crate::catalog::{Catalog, DatabaseSchema};
use crate::chunk::ParquetChunk;
use crate::last_cache::{self, CreateCacheArguments, LastCacheProvider};
use crate::persister::PersisterImpl;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::validator::WriteValidator;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ParquetFile, Persister, Precision,
    Level0Duration, WriteBuffer, WriteLineError,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError};
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use influxdb_line_protocol::v3::SeriesValue;
use influxdb_line_protocol::FieldValue;
use influxdb3_wal::{Wal as NewWal, Wal, WalConfig, WalOp};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use observability_deps::tracing::{debug, error};
use parking_lot::{Mutex, RwLock};
use parquet_file::storage::ParquetExecInput;
use schema::Schema;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use influxdb3_wal::object_store::WalObjectStore;
use crate::write_buffer::queryable_buffer::QueryableBuffer;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parsing for line protocol failed")]
    ParseError(WriteLineError),

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[error("catalog update erorr {0}")]
    CatalogUpdateError(#[from] crate::catalog::Error),

    #[error("error from buffer segment: {0}")]
    BufferSegmentError(String),

    #[error("error from persister: {0}")]
    PersisterError(#[from] crate::persister::Error),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("walop in file {0} contained data for more than one segment, which is invalid")]
    WalOpForMultipleSegments(String),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),

    #[error("error in last cache: {0}")]
    LastCacheError(#[from] last_cache::Error),

    #[error("tried accessing database and table that do not exist")]
    DbDoesNotExist,

    #[error("tried accessing database and table that do not exist")]
    TableDoesNotExist,

    #[error("error from wal: {0}")]
    WalError(#[from] influxdb3_wal::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl<T> {
    catalog: Arc<Catalog>,
    persister: Arc<PersisterImpl>,
    parquet_cache: Arc<ParquetCache>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    level_0_duration: Level0Duration,
    wal: Arc<dyn Wal>,
    #[allow(dead_code)]
    time_provider: Arc<T>,
    last_cache: Arc<LastCacheProvider>,
}

impl<T: TimeProvider> WriteBufferImpl<T> {
    pub async fn new(
        persister: Arc<PersisterImpl>,
        time_provider: Arc<T>,
        level_0_duration: Level0Duration,
        executor: Arc<iox_query::exec::Executor>,
        wal_config: WalConfig,
    ) -> Result<Self> {
        let now = time_provider.now();

        // load up the catalog, the snapshots, and replay the wal into the in memory buffer

        let last_cache = Arc::new(LastCacheProvider::new());

        let catalog = persister.load_catalog().await?;
        let catalog = Arc::new(catalog.map(|c| Catalog::from_inner(c.catalog)).unwrap_or_else(|| Catalog::new()));
        let persisted_snapshots = persister.load_snapshots(1000).await?;
        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(persisted_snapshots));
        let queryable_buffer = Arc::new(QueryableBuffer::new(executor, Arc::clone(&catalog), Arc::clone(&last_cache)));
        let wal = WalObjectStore::new(persister.object_store(), Arc::clone(&queryable_buffer), wal_config);
        wal.replay()?;
        let wal: Arc<dyn Wal> = Arc::new(wal);

        Ok(Self {
            catalog,
            parquet_cache: Arc::new(ParquetCache::new(&persister.mem_pool)),
            persister,
            wal,
            time_provider,
            level_0_duration,
            last_cache,
            persisted_files,
            buffer: queryable_buffer,
        })
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        Arc::clone(&self.persisted_files)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
            .v1_parse_lines_and_update_schema(lp, accept_partial)?
            .convert_lines_to_buffer(ingest_time, self.level_0_duration, precision);

        let op = WalOp::Write(result.valid_data);
        self.wal.write_op(op).await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            index_count: result.index_count,
        })
    }

    async fn write_lp_v3(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
            .v3_parse_lines_and_update_schema(lp, accept_partial)?
            .convert_lines_to_buffer(ingest_time, self.level_0_duration, precision);

        let op = WalOp::Write(result.valid_data);
        self.wal.write_op(op).await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            index_count: result.index_count,
        })
    }

    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self
            .catalog
            .db_schema(database_name)
            .ok_or_else(|| DataFusionError::Execution(format!("db {} not found", database_name)))?;

        let table_schema = {
            let table = db_schema.tables.get(table_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "table {} not found in db {}",
                    table_name, database_name
                ))
            })?;

            table.schema.clone()
        };

        let object_store_url = self.persister.object_store_url();

        let segment_state = self.segment_state.read();
        let mut chunks = segment_state.get_table_chunks(
            db_schema,
            table_name,
            filters,
            projection,
            object_store_url.clone(),
            self.persister.object_store(),
            ctx,
        )?;
        let parquet_files = self.persisted_files.get_files(database_name, table_name);

        let mut chunk_order = chunks.len() as i64;

        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_schema,
                object_store_url.clone(),
                Arc::clone(&self.persister.object_store()),
                chunk_order,
            );

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        // Get any cached files and add them to the query
        // This is mostly the same as above, but we change the object store to
        // point to the in memory cache
        for parquet_file in self
            .parquet_cache
            .get_parquet_files(database_name, table_name)
        {
            let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
            let partition_id = data_types::partition::TransitionPartitionId::new(
                data_types::TableId::new(0),
                &partition_key,
            );

            let chunk_stats = create_chunk_statistics(
                Some(parquet_file.row_count as usize),
                &table_schema,
                Some(parquet_file.timestamp_min_max()),
                &NoColumnRanges,
            );

            let location = ObjPath::from(parquet_file.path.clone());

            let parquet_exec = ParquetExecInput {
                object_store_url: object_store_url.clone(),
                object_meta: ObjectMeta {
                    location,
                    last_modified: Default::default(),
                    size: parquet_file.size_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                object_store: Arc::clone(&self.parquet_cache.object_store()),
            };

            let parquet_chunk = ParquetChunk {
                schema: table_schema.clone(),
                stats: Arc::new(chunk_stats),
                partition_id,
                sort_key: None,
                id: ChunkId::new(),
                chunk_order: ChunkOrder::new(chunk_order),
                parquet_exec,
            };

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }

    pub async fn cache_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, None)
            .await?)
    }

    pub async fn update_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        path: ObjPath,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, Some(path))
            .await?)
    }

    pub async fn remove_parquet(&self, path: ObjPath) -> Result<(), Error> {
        Ok(self.parquet_cache.remove_parquet_file(path).await?)
    }

    pub async fn purge_cache(&self) -> Result<(), Error> {
        Ok(self.parquet_cache.purge_cache().await?)
    }

    /// Create a new last-N-value cache in the specified database and table, along with the given
    /// parameters.
    ///
    /// Returns the name of the newly created cache.
    #[allow(clippy::too_many_arguments)]
    pub fn create_last_cache(
        &self,
        db_name: impl Into<String>,
        tbl_name: impl Into<String>,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<String>>,
        value_columns: Option<Vec<String>>,
    ) -> Result<String, Error> {
        let db_name = db_name.into();
        let tbl_name = tbl_name.into();
        let cache_name = cache_name.map(Into::into);
        let db_schema = self
            .catalog()
            .db_schema(&db_name)
            .ok_or(Error::DbDoesNotExist)?;
        let schema = db_schema
            .get_table_schema(&tbl_name)
            .ok_or(Error::TableDoesNotExist)?
            .clone();
        self.last_cache
            .create_cache(CreateCacheArguments {
                db_name,
                tbl_name,
                schema,
                cache_name,
                count,
                ttl,
                key_columns,
                value_columns,
            })
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn get_table_record_batches(
        &self,
        datbase_name: &str,
        table_name: &str,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        let db_schema = self.catalog.db_schema(datbase_name).unwrap();
        let table = db_schema.tables.get(table_name).unwrap();
        let schema = table.schema.clone();

        let segment_state = self.segment_state.read();
        segment_state.open_segments_table_record_batches(datbase_name, table_name, &schema)
    }
}

pub(crate) fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    // TODO: update persisted segments to serialize their key to use here
    let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
    let partition_id = data_types::partition::TransitionPartitionId::new(
        data_types::TableId::new(0),
        &partition_key,
    );

    let chunk_stats = create_chunk_statistics(
        Some(parquet_file.row_count as usize),
        table_schema,
        Some(parquet_file.timestamp_min_max()),
        &NoColumnRanges,
    );

    let location = ObjPath::from(parquet_file.path.clone());

    let parquet_exec = ParquetExecInput {
        object_store_url,
        object_meta: ObjectMeta {
            location,
            last_modified: Default::default(),
            size: parquet_file.size_bytes as usize,
            e_tag: None,
            version: None,
        },
        object_store,
    };

    ParquetChunk {
        schema: table_schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id,
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(chunk_order),
        parquet_exec,
    }
}

#[async_trait]
impl<W: Wal, T: TimeProvider> Bufferer for WriteBufferImpl<W, T> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp_v3(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn wal(&self) -> Option<Arc<impl Wal>> {
        self.wal.clone()
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog()
    }

    fn last_cache(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }
}

impl<W: Wal, T: TimeProvider> ChunkContainer for WriteBufferImpl<W, T> {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

impl<T: TimeProvider> WriteBuffer for WriteBufferImpl<T> {}

// #[derive(Debug, Default)]
// pub(crate) struct TableBatch {
//     #[allow(dead_code)]
//     pub(crate) name: String,
//     pub(crate) rows: Vec<Row>,
// }
//
// #[derive(Clone, Debug, Eq, PartialEq)]
// pub(crate) struct Row {
//     pub(crate) time: i64,
//     pub(crate) fields: Vec<Field>,
// }
//
// #[derive(Clone, Debug, Eq, PartialEq)]
// pub(crate) struct Field {
//     pub(crate) name: String,
//     pub(crate) value: FieldData,
// }
//
// #[derive(Clone, Debug)]
// pub(crate) enum FieldData {
//     Timestamp(i64),
//     Key(String),
//     Tag(String),
//     String(String),
//     Integer(i64),
//     UInteger(u64),
//     Float(f64),
//     Boolean(bool),
// }
//
// impl PartialEq for FieldData {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (FieldData::Timestamp(a), FieldData::Timestamp(b)) => a == b,
//             (FieldData::Tag(a), FieldData::Tag(b)) => a == b,
//             (FieldData::Key(a), FieldData::Key(b)) => a == b,
//             (FieldData::String(a), FieldData::String(b)) => a == b,
//             (FieldData::Integer(a), FieldData::Integer(b)) => a == b,
//             (FieldData::UInteger(a), FieldData::UInteger(b)) => a == b,
//             (FieldData::Float(a), FieldData::Float(b)) => a == b,
//             (FieldData::Boolean(a), FieldData::Boolean(b)) => a == b,
//             _ => false,
//         }
//     }
// }
//
// impl Eq for FieldData {}
//
// impl<'a> From<&SeriesValue<'a>> for FieldData {
//     fn from(sk: &SeriesValue<'a>) -> Self {
//         match sk {
//             SeriesValue::String(s) => Self::Key(s.to_string()),
//         }
//     }
// }
//
// impl<'a> From<FieldValue<'a>> for FieldData {
//     fn from(value: FieldValue<'a>) -> Self {
//         match value {
//             FieldValue::I64(v) => Self::Integer(v),
//             FieldValue::U64(v) => Self::UInteger(v),
//             FieldValue::F64(v) => Self::Float(v),
//             FieldValue::String(v) => Self::String(v.to_string()),
//             FieldValue::Boolean(v) => Self::Boolean(v),
//         }
//     }
// }
//
// #[derive(Debug)]
// pub(crate) struct ValidSegmentedData {
//     pub(crate) database_name: NamespaceName<'static>,
//     pub(crate) segment_start: Time,
//     pub(crate) table_batches: HashMap<String, TableBatch>,
//     pub(crate) wal_op: WalOp,
//     /// The sequence number of the catalog before any updates were applied based on this write.
//     pub(crate) starting_catalog_sequence_number: SequenceNumber,
// }
//
// #[derive(Debug, Default)]
// pub(crate) struct TableBatchMap<'a> {
//     pub(crate) lines: Vec<&'a str>,
//     pub(crate) table_batches: HashMap<String, TableBatch>,
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persister::PersisterImpl;
    use crate::wal::WalImpl;
    use crate::{LpWriteOp, SegmentId, SequenceNumber, WalOpBatch};
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use datafusion_util::config::register_iox_object_store;
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    #[test]
    fn parse_lp_into_buffer() {
        let catalog = Arc::new(Catalog::new());
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        WriteValidator::initialize(db_name, Arc::clone(&catalog))
            .unwrap()
            .v1_parse_lines_and_update_schema(lp, false)
            .unwrap()
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                Level0Duration::new_5m(),
                Precision::Nanosecond,
            );

        let db = catalog.db_schema("foo").unwrap();

        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.tables.get("cpu").unwrap().num_columns(), 3);
        assert_eq!(db.tables.get("foo").unwrap().num_columns(), 2);
    }

    #[tokio::test]
    async fn buffers_and_persists_to_wal() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = WalImpl::new(dir.clone()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = Level0Duration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            Some(Arc::new(wal)),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();

        let summary = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        assert_eq!(summary.line_count, 1);
        assert_eq!(summary.field_count, 1);
        assert_eq!(summary.index_count, 0);

        // ensure the data is in the buffer
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &actual);

        // ensure the data is in the wal
        let wal = WalImpl::new(dir).unwrap();
        let mut reader = wal.open_segment_reader(SegmentId::new(1)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        let expected_batch = WalOpBatch {
            sequence_number: SequenceNumber::new(1),
            ops: vec![WalOp::LpWrite(LpWriteOp {
                db_name: "foo".to_string(),
                lp: "cpu bar=1 10".to_string(),
                default_time: 123,
                precision: Precision::Nanosecond,
            })],
        };
        assert_eq!(batch, expected_batch);

        // ensure we load state from the persister
        let write_buffer = WriteBufferImpl::new(
            persister,
            Some(Arc::new(wal)),
            time_provider,
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_buffered_and_persisted_data() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = Level0Duration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // advance the time and wait for it to persist
        time_provider.set(Time::from_timestamp(800, 0).unwrap());
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            if !write_buffer
                .persisted_files
                .get_files("foo", "cpu")
                .is_empty()
            {
                break;
            }
        }

        // nothing should be open at this point
        assert!(write_buffer
            .segment_state
            .read()
            .open_segment_times()
            .is_empty());

        // verify we get the persisted data
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // now write some into the next segment we're in and verify we get both buffer and persisted
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2",
                Time::from_timestamp(900, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now reload the buffer and verify that we get persisted and the buffer again
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal,
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now add to the buffer and verify that we still only get two chunks
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3",
                Time::from_timestamp(950, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 3.0 | 1970-01-01T00:15:50Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sets_starting_catalog_number_on_new_segment() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = Level0Duration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let starting_catalog_sequence_number = write_buffer.catalog().sequence_number();

        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        // write data into the buffer that will go into a new segment
        let new_segment_time = Time::from_timestamp(360, 0).unwrap();
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1",
                new_segment_time,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:06:00Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // get the segment for the new_segment_time and validate that it has the correct starting catalog sequence number
        let state = write_buffer.segment_state.read();
        let segment_start_time = Level0Duration::new_5m().start_time(new_segment_time.timestamp());
        let segment = state.segment_for_time(segment_start_time).unwrap();
        assert_eq!(
            segment.starting_catalog_sequence_number(),
            starting_catalog_sequence_number
        );
    }

    async fn get_table_batches(
        write_buffer: &WriteBufferImpl<WalImpl, MockProvider>,
        database_name: &str,
        table_name: &str,
        ctx: &IOxSessionContext,
    ) -> Vec<RecordBatch> {
        let chunks = write_buffer
            .get_table_chunks(database_name, table_name, &[], None, &ctx.inner().state())
            .unwrap();
        let mut batches = vec![];
        for chunk in chunks {
            let chunk = chunk
                .data()
                .read_to_batches(chunk.schema(), ctx.inner())
                .await;
            batches.extend(chunk);
        }
        batches
    }
}
