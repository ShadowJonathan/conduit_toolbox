use std::path::Path;

use rusqlite::{self, Connection, DatabaseName::Main};

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

    pub fn table<'a>(&'a mut self, string: String) -> rusqlite::Result<SqliteTable<'a>> {
        // taken from src/database/abstraction/sqlite.rs
        self.0.execute(format!("CREATE TABLE IF NOT EXISTS {} ( \"key\" BLOB PRIMARY KEY, \"value\" BLOB NOT NULL )", &string).as_str(), [])?;

        Ok(SqliteTable(&mut self.0, string))
    }
}

pub struct SqliteTable<'a>(&'a mut Connection, String);

impl SqliteTable<'_> {
    pub fn batch_insert(
        &mut self,
        batch: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> rusqlite::Result<()> {
        let tx = self.0.transaction()?;
        let sql_s = format!("INSERT INTO {} (key, value) VALUES (?, ?)", &self.1);
        let sql = sql_s.as_str();

        for (k, v) in batch {
            tx.execute(sql, rusqlite::params![k, v])?;
        }

        tx.commit()
    }
}
