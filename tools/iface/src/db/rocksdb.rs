use std::{path::Path, sync::Arc};

use super::{Database, Segment};
use rocksdb::{DBWithThreadMode, MultiThreaded};

pub fn options() -> rocksdb::Options {
    let mut db_opts = rocksdb::Options::default();

    db_opts.create_if_missing(true);
    db_opts.set_max_open_files(512);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    db_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
    db_opts.set_level_compaction_dynamic_level_bytes(true);
    db_opts.set_bytes_per_sync(1048576);
    db_opts.set_keep_log_file_num(100);
    db_opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);

    let mut block_based_options = rocksdb::BlockBasedOptions::default();
    block_based_options.set_bloom_filter(10.0, false);
    block_based_options.set_block_size(4 * 1024);
    block_based_options.set_cache_index_and_filter_blocks(true);
    block_based_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_based_options.set_optimize_filters_for_memory(true);

    db_opts.set_block_based_table_factory(&block_based_options);

    db_opts
}

pub fn new_conn<P: AsRef<Path>>(path: P) -> Result<RocksDB, rocksdb::Error> {
    let opts = options();

    let cfs = DBWithThreadMode::<MultiThreaded>::list_cf(&opts, &path).unwrap_or_default();

    let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
        &opts,
        &path,
        cfs.iter().map(|name| {
            let mut options = opts.clone();
            let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(1);
            options.set_prefix_extractor(prefix_extractor);

            rocksdb::ColumnFamilyDescriptor::new(name, options)
        }),
    )?;

    Ok(RocksDB {
        rocks: db,
        old_cfs: cfs,
    })
}

pub struct RocksDB {
    rocks: DBWithThreadMode<MultiThreaded>,
    old_cfs: Vec<String>,
}

impl Database for RocksDB {
    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>> {
        let string = String::from_utf8(name).unwrap();

        // Create if it didn't exist
        if !self.old_cfs.contains(&string) {
            let mut options = options();

            let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(1);
            options.set_prefix_extractor(prefix_extractor);

            let _ = self.rocks.create_cf(&string, &options);
            println!("created cf");
        }

        Some(Box::new(RocksDBCF {
            db: self,
            name: string,
        }))
    }

    fn names<'a>(&'a self) -> Vec<Vec<u8>> {
        self.old_cfs
            .iter()
            .filter(|&v| &*v != "default")
            .map(|v| v.as_bytes().to_vec())
            .collect()
    }

    fn flush(&mut self) {
        self.rocks.flush().unwrap()
    }
}

impl Drop for RocksDB {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.rocks.flush();
    }
}

pub struct RocksDBCF<'a> {
    db: &'a mut RocksDB,
    name: String,
}

impl RocksDBCF<'_> {
    fn cf(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.rocks.cf_handle(&self.name).unwrap()
    }
}

impl<'r> Segment for RocksDBCF<'r> {
    fn batch_insert<'a>(
        &'a mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    ) -> anyhow::Result<()> {
        let cf = self.cf();
        for (key, value) in batch {
            self.db.rocks.put_cf(&cf, key, value)?;
        }

        Ok(())
    }

    fn get_iter(&mut self) -> Box<dyn super::SegmentIter + '_> {
        Box::new(RocksDBCFIter(self))
    }
}

pub struct RocksDBCFIter<'a>(&'a RocksDBCF<'a>);

impl super::SegmentIter for RocksDBCFIter<'_> {
    fn iter<'a>(&'a mut self) -> super::KVIter<'a> {
        Box::new(
            self.0
                .db
                .rocks
                .iterator_cf(&self.0.cf(), rocksdb::IteratorMode::Start)
                .map(|r| {
                    let (k, v) = r.expect("we expect rocksdb to give us good rows only");
                    (Vec::from(k), Vec::from(v))
                }),
        )
    }
}
