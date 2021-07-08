use std::path::Path;

use sled;

pub fn new_db<P: AsRef<Path>>(path: P) -> sled::Result<sled::Db> {
    sled::Config::default()
        .path(path)
        .use_compression(true)
        .open()
}

pub struct SledDB(sled::Db);

impl SledDB {
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (Vec<u8>, impl Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a)> {
        self.0
            .tree_names()
            .into_iter()
            .map(|v| v.to_vec())
            .filter_map(move |v| {
                let t = if let Ok(t) = self.0.open_tree(&v) {
                    t
                } else {
                    return None;
                };

                let i = t.into_iter().filter_map(|r| {
                    if let Ok(t) = r {
                        Some((t.0.to_vec(), t.1.to_vec()))
                    } else {
                        None
                    }
                });

                Some((v, i))
            })
    }

    pub fn new(db: sled::Db) -> Self {
        Self(db)
    }
}
