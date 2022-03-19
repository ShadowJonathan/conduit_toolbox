use super::{Database, KVIter, Segment, SegmentIter};
use persy::{ByteVec, Persy};
use std::path::Path;

pub fn new_db<P: AsRef<Path>>(path: P) -> anyhow::Result<PersyDB> {
    let path = Path::new("./db.persy").join(path);

    let persy = persy::OpenOptions::new()
        .create(true)
        .config(persy::Config::new())
        .open(&path)?;

    Ok(PersyDB { persy })
}

pub struct PersyDB {
    persy: Persy,
}

impl Database for PersyDB {
    fn names<'a>(&'a self) -> Vec<Vec<u8>> {
        self.persy
            .list_indexes()
            .unwrap()
            .iter()
            .map(|(s, _)| s.as_bytes().to_vec())
            .collect()
    }

    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>> {
        let string = String::from_utf8(name).unwrap();

        if !self.persy.exists_index(&string).unwrap() {
            use persy::ValueMode;

            let mut tx = self.persy.begin().unwrap();
            tx.create_index::<ByteVec, ByteVec>(&string, ValueMode::Replace)
                .unwrap();
            tx.prepare().unwrap().commit().unwrap();
        }

        Some(Box::new(PersySeg {
            db: self,
            name: string,
        }))
    }

    fn flush(&mut self) {
        // NOOP
    }
}

pub struct PersySeg<'a> {
    db: &'a mut PersyDB,
    name: String,
}

impl<'r> Segment for PersySeg<'r> {
    fn batch_insert<'a>(
        &'a mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    ) -> anyhow::Result<()> {
        let mut tx = self.db.persy.begin()?;
        for (key, value) in batch {
            tx.put::<ByteVec, ByteVec>(
                &self.name,
                ByteVec::from(key.clone()),
                ByteVec::from(value),
            )?;
        }
        tx.prepare()?.commit()?;

        Ok(())
    }

    fn get_iter<'a>(&'a mut self) -> Box<dyn SegmentIter + 'a> {
        Box::new(PersySegIter(self, &self.name))
    }
}

pub struct PersySegIter<'a>(&'a PersySeg<'a>, &'a str);

impl<'r> SegmentIter for PersySegIter<'r> {
    fn iter<'a>(&'a mut self) -> KVIter<'a> {
        Box::new(
            self.0
                .db
                .persy
                .range::<ByteVec, ByteVec, _>(self.1, ..)
                .unwrap()
                .filter_map(|(k, v)| {
                    v.into_iter()
                        .map(|val| ((*k).to_owned().into(), (*val).to_owned().into()))
                        .next()
                }),
        )
    }
}
