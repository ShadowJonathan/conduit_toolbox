#[cfg(feature = "heed")]
pub mod heed;
#[cfg(feature = "persy")]
pub mod persy;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
#[cfg(feature = "sled")]
pub mod sled;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use itertools::Itertools;

pub type KVIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;

pub type TreeKVIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, KVIter<'a>)> + 'a>;

#[derive(Clone, Copy)]
pub struct Config {
    pub ignore_broken_rows: bool,
}

pub trait Database {
    fn names<'a>(&'a self) -> Vec<Vec<u8>>;

    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>>; // change return type to Result

    fn flush(&mut self);
}

pub trait Segment {
    fn batch_insert<'a>(
        &'a mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    ) -> anyhow::Result<()>;

    fn get_iter<'a>(&'a mut self) -> Box<dyn SegmentIter + 'a>;
}

pub trait SegmentIter {
    fn iter<'a>(&'a mut self) -> KVIter<'a>;
}

pub fn copy_database(
    src: &mut dyn Database,
    dst: &mut dyn Database,
    chunk_size: usize,
) -> anyhow::Result<()> {
    // todo remove unwraps
    for seg_name in src.names() {
        drop(dbg!(String::from_utf8(seg_name.clone())));

        let mut src_seg = src.segment(seg_name.clone()).unwrap();

        let mut dst_seg = dst.segment(seg_name).unwrap();

        let mut src_seg_iter = src_seg.get_iter();

        let i = src_seg_iter.iter();

        let mut x: usize = 0;

        for chunk in &i.chunks(chunk_size) {
            dbg!(&x);
            dst_seg.batch_insert(Box::new(chunk))?;
            x += chunk_size;
        }

        drop(dst_seg);
        drop(src_seg_iter);
        drop(src_seg);

        dst.flush();
    }

    Ok(())
}
