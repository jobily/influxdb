use std::any::Any;
use std::sync::Arc;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use iox_query::exec::Executor;
use object_store::ObjectStore;
use tokio::sync::oneshot::Receiver;
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier};
use crate::catalog::{Catalog, DatabaseSchema};
use crate::last_cache::LastCacheProvider;

#[derive(Debug)]
pub(crate) struct QueryableBuffer {
    executor: Arc<Executor>,
    catalog: Arc<Catalog>,
    last_cache_provider: Arc<LastCacheProvider>,
}

impl QueryableBuffer {
    pub(crate) fn new(executor: Arc<Executor>, catalog: Arc<Catalog>, last_cache_provider: Arc<LastCacheProvider>) -> Self {
        Self {
            executor,
            catalog,
            last_cache_provider,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        object_store_url: ObjectStoreUrl,
        object_store: Arc<dyn ObjectStore>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let table = db_schema
            .tables
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;

        let arrow_schema: SchemaRef = match projection {
            Some(projection) => Arc::new(table.schema.as_arrow().project(projection).unwrap()),
            None => table.schema.as_arrow(),
        };

        let schema = schema::Schema::try_from(Arc::clone(&arrow_schema))
            .map_err(|e| DataFusionError::Execution(format!("schema error {}", e)))?;

        let mut chunks: Vec<Arc<dyn QueryChunk>> = vec![];

        for segment in self.segments.values() {
            // output the older persisted stuff first
            if let Some(table_paruqet_files) =
                segment.table_persisted_parquet_files(&db_schema.name, table_name)
            {
                for parquet_file in &table_paruqet_files.parquet_files {
                    let parquet_chunk = parquet_chunk_from_file(
                        parquet_file,
                        &schema,
                        object_store_url.clone(),
                        Arc::clone(&object_store),
                        chunks
                            .len()
                            .try_into()
                            .expect("should never have this many chunks"),
                    );

                    chunks.push(Arc::new(parquet_chunk));
                }
            }

            // now add the in-memory stuff
            if let Some(batches) = segment.table_record_batches(
                &db_schema.name,
                table_name,
                Arc::clone(&arrow_schema),
                filters,
            ) {
                let batches = batches.map_err(|e| {
                    DataFusionError::Execution(format!("error getting batches {}", e))
                })?;
                let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();

                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &schema,
                    Some(segment.segment_range().timestamp_min_max()),
                    &NoColumnRanges,
                );

                chunks.push(Arc::new(BufferChunk {
                    batches,
                    schema: schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::new(
                        TableId::new(0),
                        segment.segment_key(),
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: ChunkOrder::new(
                        chunks
                            .len()
                            .try_into()
                            .expect("should never have this many chunks"),
                    ),
                }));
            }
        }
    }
}

impl WalFileNotifier for QueryableBuffer {
    fn notify(&self, write: WalContents) {
        todo!()
    }

    async fn notify_and_snapshot(&self, write: WalContents, snapshot_details: SnapshotDetails) -> Receiver<SnapshotDetails> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }
}