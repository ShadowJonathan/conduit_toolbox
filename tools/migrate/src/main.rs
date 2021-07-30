use clap::{App, Arg};
use conduit_iface::db::{self, copy_database, heed::HeedDB, sled::SledDB, sqlite::SqliteDB};
use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

enum Database {
    Sled(SledDB),
    Sqlite(SqliteDB),
    Heed(HeedDB),
}

impl Database {
    fn new(name: &str, path: PathBuf) -> anyhow::Result<Self> {
        Ok(match name {
            "sled" => Self::Sled(SledDB::new(db::sled::new_db(path)?)),
            "heed" => Self::Heed(HeedDB::new(db::heed::new_db(path)?)),
            "sqlite" => Self::Sqlite(SqliteDB::new(db::sqlite::new_conn(path)?)),
            _ => panic!("unknown database type: {}", name),
        })
    }
}

impl Deref for Database {
    type Target = dyn db::Database;

    fn deref(&self) -> &Self::Target {
        match self {
            Database::Sled(db) => db,
            Database::Sqlite(db) => db,
            Database::Heed(db) => db,
        }
    }
}

impl DerefMut for Database {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Database::Sled(db) => db,
            Database::Sqlite(db) => db,
            Database::Heed(db) => db,
        }
    }
}

const DATABASES: &[&str] = &["heed", "sqlite", "sled"];

fn main() -> anyhow::Result<()> {
    let matches = App::new("Conduit Sled to Sqlite Migrator")
        .arg(
            Arg::with_name("from_dir")
                .short("s")
                .long("from-dir")
                .takes_value(true)
                .long_help("Sets the directory to grab the database from\nWill default to \".\""),
        )
        .arg(
            Arg::with_name("to_dir")
                .short("d")
                .long("to-dir")
                .takes_value(true)
                .long_help("Sets the destination directory\nWill default to from_dir"),
        )
        .arg(
            Arg::with_name("from")
                .short("f")
                .long("from")
                .long_help(
                    format!(
                        "The type of database to convert from\nExample: {}",
                        DATABASES.join(", ")
                    )
                    .as_str(),
                )
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("to")
                .short("t")
                .long("to")
                .long_help(
                    format!(
                        "The type of database to convert to\nExample: {}",
                        DATABASES.join(", ")
                    )
                    .as_str(),
                )
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let src_dir = matches.value_of("from_dir").unwrap_or(".");

    let dst_dir = matches.value_of("to_dir");

    let src_dir = Path::new(src_dir).canonicalize()?;

    if !src_dir.is_dir() {
        return Err(anyhow::anyhow!("source path must be directory"));
    }

    let dst_dir = match dst_dir {
        None => Ok(src_dir.clone()),
        Some(dir) => {
            let p = Path::new(dir).canonicalize()?;
            if !p.is_dir() {
                Err(anyhow::anyhow!("destination path must be directory"))
            } else {
                Ok(p)
            }
        }
    }?;

    dbg!(&src_dir, &dst_dir);

    let mut src_db = Database::new(matches.value_of("from").unwrap(), src_dir)?;

    let mut dst_db = Database::new(matches.value_of("to").unwrap(), dst_dir)?;

    copy_database(&mut *src_db, &mut *dst_db, 1000)?;

    Ok(())
}
