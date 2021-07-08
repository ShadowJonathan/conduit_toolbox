mod sled;
mod sqlite;
use std::path::Path;

use clap::{App, Arg};

use crate::{sled::SledDB, sqlite::SqliteDB};

use itertools::Itertools;

fn main() -> anyhow::Result<()> {
    let matches = App::new("Conduit Sled to Sqlite Migrator")
        .arg(
            Arg::with_name("DIR")
                .long_help("Sets the directory to grab the database from\nWill default to \".\"")
                .index(1),
        )
        .arg(
            Arg::with_name("NEW_DIR")
                .long_help("Sets the destination directory\nWill default to DIR")
                .index(2),
        )
        .get_matches();

    let source_dir = matches.value_of("DIR").unwrap_or(".");

    let dest_dir = matches.value_of("NEW_DIR");

    let source_dir = Path::new(source_dir).canonicalize()?;

    if !source_dir.is_dir() {
        return Err(anyhow::anyhow!("source path must be directory"));
    }

    let dest_dir = match dest_dir {
        None => Ok(source_dir.clone()),
        Some(dir) => {
            let p = Path::new(dir).canonicalize()?;
            if !p.is_dir() {
                Err(anyhow::anyhow!("destination path must be directory"))
            } else {
                Ok(p)
            }
        }
    }?;

    dbg!(&source_dir, &dest_dir);

    let sled = SledDB::new(crate::sled::new_db(source_dir)?);

    let mut sqlite = SqliteDB::new(sqlite::new_conn(dest_dir)?);

    for (tree, i) in sled.iter() {
        let tree = String::from_utf8(tree)?;

        dbg!(&tree);

        let mut t = sqlite.table(tree)?;

        let mut x: u32 = 0;

        for chunk in &i.chunks(1000) {
            dbg!(&x);
            t.batch_insert(chunk)?;
            x += 1000;
        }
    }

    Ok(())
}
