pub mod sled;
pub mod sqlite;

use itertools::Itertools;

pub type KVIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;

pub type TreeKVIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, KVIter<'a>)> + 'a>;

pub trait Database {
    fn iter<'a>(&'a self) -> TreeKVIter<'a>;

    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>>; // change return type to Result
}

pub trait Segment {
    fn batch_insert<'a>(
        &'a mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    ) -> anyhow::Result<()>;
}

pub fn copy_database(
    src: &impl Database,
    dst: &mut impl Database,
    chunk_size: usize,
) -> anyhow::Result<()> {
    for (tree, i) in src.iter() {
        dbg!(&tree);

        let mut t = dst.segment(tree).unwrap(); // todo remove unwrap

        let mut x: usize = 0;

        for chunk in &i.chunks(chunk_size) {
            dbg!(&x);
            t.batch_insert(Box::new(chunk))?;
            x += chunk_size;
        }
    }

    Ok(())
}
