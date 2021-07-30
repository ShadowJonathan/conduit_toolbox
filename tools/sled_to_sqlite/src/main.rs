use std::path::Path;

use clap::{App, Arg};

use conduit_iface::db::{copy_database, sled, sqlite};

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

    let mut sled = sled::SledDB::new(sled::new_db(source_dir)?);

    let mut sqlite = sqlite::SqliteDB::new(sqlite::new_conn(dest_dir)?);

    copy_database(&mut sled, &mut sqlite, 1000)?;

    Ok(())
}
