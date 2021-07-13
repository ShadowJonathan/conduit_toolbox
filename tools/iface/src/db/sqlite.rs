use std::path::Path;

use rusqlite::{self, Connection, DatabaseName::Main};

use super::{Database, Segment};

pub fn new_conn<P: AsRef<Path>>(path: P) -> rusqlite::Result<Connection> {
    let path = path.as_ref().join("conduit.db");
    let conn = Connection::open(path)?;

    conn.pragma_update(Some(Main), "journal_mode", &"WAL".to_owned())?;

    Ok(conn)
}

pub struct SqliteDB(Connection);

impl SqliteDB {
    pub fn new(conn: Connection) -> Self {
        Self(conn)
    }
}

impl Database for SqliteDB {
    fn iter<'a>(&'a self) -> super::TreeKVIter<'a> {
        todo!("iterate over tables, pick only tables that have columns 'key' and 'value', then iterate over that with values")
    }

    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>> {
        let string = String::from_utf8(name).unwrap();
        // taken from src/database/abstraction/sqlite.rs
        self.0.execute(format!("CREATE TABLE IF NOT EXISTS {} ( \"key\" BLOB PRIMARY KEY, \"value\" BLOB NOT NULL )", &string).as_str(), []).unwrap();

        Some(Box::new(SqliteTable(&mut self.0, string)))
    }
}

pub struct SqliteTable<'a>(&'a mut Connection, String);

impl Segment for SqliteTable<'_> {
    fn batch_insert(
        &mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>,
    ) -> anyhow::Result<()> {
        let tx = self.0.transaction()?;
        let sql_s = format!("INSERT INTO {} (key, value) VALUES (?, ?)", &self.1);
        let sql = sql_s.as_str();

        for (k, v) in batch {
            tx.execute(sql, rusqlite::params![k, v])?;
        }

        tx.commit().map_err(Into::into)
    }
}
