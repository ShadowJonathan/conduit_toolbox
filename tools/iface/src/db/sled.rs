use std::path::Path;

use sled::{Batch, Config, Db, Result, Tree};

use super::{Database, KVIter, Segment, TreeKVIter};

pub fn new_db<P: AsRef<Path>>(path: P) -> Result<Db> {
    Config::default().path(path).use_compression(true).open()
}

pub struct SledDB(Db);

impl SledDB {
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

impl Database for SledDB {
    fn iter<'a>(&'a self) -> TreeKVIter<'a> {
        Box::new(
            self.0
                .tree_names()
                .into_iter()
                .map(|v| v.to_vec())
                .filter_map(move |v| {
                    if let Ok(t) = self.0.open_tree(&v) {
                        Some((v, t))
                    } else {
                        None
                    }
                })
                .map(|(v, t): (Vec<u8>, Tree)| -> (Vec<u8>, KVIter<'a>) {
                    let i = t.into_iter().filter_map(|r| {
                        if let Ok(t) = r {
                            Some((t.0.to_vec(), t.1.to_vec()))
                        } else {
                            None
                        }
                    });

                    (v, Box::new(i))
                }),
        )
    }

    fn segment(&mut self, name: Vec<u8>) -> Option<Box<dyn Segment>> {
        self.0
            .open_tree(name)
            .ok()
            .map(|t| -> Box<dyn Segment> { Box::new(t) })
    }
}

impl Segment for Tree {
    fn batch_insert(
        &mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>,
    ) -> anyhow::Result<()> {
        let mut sled_batch = Batch::default();

        for (k, v) in batch {
            sled_batch.insert(k, v)
        }

        self.apply_batch(sled_batch).map_err(Into::into)
    }
}
