use clap::{App, Arg};
use conduit_iface::db::{self, copy_database, Config};
use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

enum Database {
    #[cfg(feature = "heed")]
    Heed(db::heed::HeedDB),
    #[cfg(feature = "sqlite")]
    Sqlite(db::sqlite::SqliteDB),
    #[cfg(feature = "rocksdb")]
    Rocks(db::rocksdb::RocksDB),
    #[cfg(feature = "persy")]
    Persy(db::persy::PersyDB),
}

impl Database {
    fn new(name: &str, path: PathBuf, config: Config) -> anyhow::Result<Self> {
        Ok(match name {
            #[cfg(feature = "heed")]
            "heed" => Self::Heed(db::heed::HeedDB::new(db::heed::new_db(path)?)),
            #[cfg(feature = "sqlite")]
            "sqlite" => Self::Sqlite(db::sqlite::SqliteDB::new(
                db::sqlite::new_conn(path)?,
                config,
            )),
            #[cfg(feature = "rocksdb")]
            "rocks" => Self::Rocks(db::rocksdb::new_conn(path)?),
            #[cfg(feature = "persy")]
            "persy" => Self::Persy(db::persy::new_db(path)?),
            _ => panic!("unknown database type: {}", name),
        })
    }
}

impl Deref for Database {
    type Target = dyn db::Database;

    fn deref(&self) -> &Self::Target {
        match self {
            #[cfg(feature = "heed")]
            Database::Heed(db) => db,
            #[cfg(feature = "sqlite")]
            Database::Sqlite(db) => db,
            #[cfg(feature = "rocksdb")]
            Database::Rocks(db) => db,
            #[cfg(feature = "persy")]
            Database::Persy(db) => db,
        }
    }
}

impl DerefMut for Database {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            #[cfg(feature = "heed")]
            Database::Heed(db) => db,
            #[cfg(feature = "sqlite")]
            Database::Sqlite(db) => db,
            #[cfg(feature = "rocksdb")]
            Database::Rocks(db) => db,
            #[cfg(feature = "persy")]
            Database::Persy(db) => db,
        }
    }
}

const DATABASES: &[&str] = &[
    #[cfg(feature = "heed")]
    "heed",
    #[cfg(feature = "sqlite")]
    "sqlite",
    #[cfg(feature = "rocksdb")]
    "rocks",
    #[cfg(feature = "persy")]
    "persy",
];

fn main() -> anyhow::Result<()> {
    let matches = App::new("Conduit Generic Migrator")
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
        .arg(
            Arg::with_name("ignore_broken_rows")
                .long("ignore-broken-rows")
                .long_help("Lossy migration methodology if parts of the database are malformed due to e.g. improper manual database surgery. Currently only applies to SQLite.")
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

    let ignore_broken_rows = matches.is_present("ignore_broken_rows");

    let config = Config { ignore_broken_rows };

    let mut src_db = Database::new(matches.value_of("from").unwrap(), src_dir, config)?;

    let mut dst_db = Database::new(matches.value_of("to").unwrap(), dst_dir, config)?;

    copy_database(&mut *src_db, &mut *dst_db, 1000)?;

    Ok(())
}
