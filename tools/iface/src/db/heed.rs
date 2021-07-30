use super::{Database, KVIter, Segment, SegmentIter};
use heed::UntypedDatabase;
use itertools::Itertools;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("There was a problem with the connection to the heed database: {0}")]
pub struct HeedError(String);

impl From<heed::Error> for HeedError {
    fn from(err: heed::Error) -> Self {
        Self(err.to_string())
    }
}

pub fn new_db<P: AsRef<Path>>(path: P) -> Result<heed::Env, HeedError> {
    let mut env_builder = heed::EnvOpenOptions::new();
    // env_builder.map_size(1024 * 1024 * 1024); // 1 Terabyte
    env_builder.max_readers(126);
    env_builder.max_dbs(128);

    Ok(env_builder.open(path)?)
}

pub struct HeedDB(heed::Env);

impl HeedDB {
    pub fn new(env: heed::Env) -> Self {
        Self(env)
    }
}

impl Database for HeedDB {
    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn super::Segment + 'a>> {
        let name = String::from_utf8(name).ok()?;

        let db: UntypedDatabase = self.0.create_database(Some(name.as_str())).ok()?;

        Some(Box::new(HeedSegment {
            env: self.0.clone(),
            db,
        }))
    }

    fn names<'a>(&'a self) -> Vec<Vec<u8>> {
        let db: UntypedDatabase = self.0.open_database(None).unwrap().unwrap();

        let txn = self.0.read_txn().unwrap();

        db.iter(&txn)
            .unwrap()
            .filter_map(|r| -> Option<(Vec<u8>, UntypedDatabase)> {
                let (k, _) = r.ok()?;

                let name = String::from_utf8(k.to_vec()).ok()?;

                if let Some(db) = (self.0.open_database(Some(name.as_str()))).ok().flatten() {
                    Some((k.to_vec(), db))
                } else {
                    None
                }
            })
            .map(|(k, _)| k)
            .collect_vec()
    }
}
pub struct HeedSegment {
    env: heed::Env,
    db: heed::UntypedDatabase,
}

impl Segment for HeedSegment {
    fn batch_insert<'a>(
        &'a mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    ) -> anyhow::Result<()> {
        let mut txn = self.env.write_txn().unwrap();

        for (k, v) in batch {
            self.db.put(&mut txn, &k.as_slice(), &v.as_slice()).unwrap();
        }

        txn.commit().unwrap();

        Ok(())
    }

    fn get_iter<'a>(&'a mut self) -> Box<dyn super::SegmentIter + 'a> {
        todo!()
    }
}

struct HeedSegmentIter<'a>(heed::RoTxn<'a>, &'a heed::UntypedDatabase);

impl SegmentIter for HeedSegmentIter<'_> {
    fn iter<'a>(&'a mut self) -> KVIter<'a> {
        Box::new(self.1.iter(&self.0).unwrap().filter_map(|r| {
            if let Ok(t) = r {
                Some((t.0.to_vec(), t.1.to_vec()))
            } else {
                None
            }
        }))
    }
}
