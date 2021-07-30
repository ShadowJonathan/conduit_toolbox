use itertools::Itertools;
use rusqlite::{self, Connection, DatabaseName::Main, Statement};
use std::{collections::HashSet, iter::FromIterator, path::Path};

use super::{Database, KVIter, Segment, SegmentIter};

pub fn new_conn<P: AsRef<Path>>(path: P) -> rusqlite::Result<Connection> {
    let path = path.as_ref().join("conduit.db");
    let conn = Connection::open(path)?;

    conn.pragma_update(Some(Main), "journal_mode", &"WAL".to_owned())?;

    Ok(conn)
}

pub struct SqliteDB {
    conn: Connection,
}

const CORRECT_TABLE_SET: &[&str] = &["key", "value"];

impl<'a> SqliteDB {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    fn valid_tables(&self) -> Vec<String> {
        self.conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .filter(|a| self.test_table(a))
            .collect()
    }

    fn test_table(&self, table: &String) -> bool {
        let set: HashSet<String> = self
            .conn
            .prepare("SELECT name FROM pragma_table_info(?)")
            .unwrap()
            .query_map([table], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        set == HashSet::from_iter(CORRECT_TABLE_SET.iter().map(|s| s.to_string()))
    }
}

impl Database for SqliteDB {
    fn names<'a>(&'a self) -> Vec<Vec<u8>> {
        self.valid_tables().into_iter().map_into().collect_vec()
    }

    fn segment<'a>(&'a mut self, name: Vec<u8>) -> Option<Box<dyn Segment + 'a>> {
        let string = String::from_utf8(name).unwrap();
        // taken from src/database/abstraction/sqlite.rs
        self.conn.execute(format!("CREATE TABLE IF NOT EXISTS {} ( \"key\" BLOB PRIMARY KEY, \"value\" BLOB NOT NULL )", &string).as_str(), []).unwrap();

        Some(Box::new(SqliteSegment {
            conn: &mut self.conn,
            name: string,
        }))
    }
}

pub struct SqliteSegment<'a> {
    conn: &'a mut Connection,
    name: String,
}

impl Segment for SqliteSegment<'_> {
    fn batch_insert(
        &mut self,
        batch: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + '_>,
    ) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        let sql_s = format!("INSERT INTO {} (key, value) VALUES (?, ?)", &self.name);
        let sql = sql_s.as_str();

        for (k, v) in batch {
            tx.execute(sql, rusqlite::params![k, v])?;
        }

        tx.commit().map_err(Into::into)
    }

    fn get_iter(&mut self) -> Box<dyn super::SegmentIter + '_> {
        Box::new(SqliteSegmentIter(
            self.conn
                .prepare(format!("SELECT key, value FROM {}", self.name).as_str())
                .unwrap(),
        ))
    }
}

struct SqliteSegmentIter<'a>(Statement<'a>);

impl SegmentIter for SqliteSegmentIter<'_> {
    fn iter<'f>(&'f mut self) -> KVIter<'f> {
        Box::new(
            self.0
                .query_map([], |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))
                .unwrap()
                .map(|r| r.unwrap()),
        )
    }
}
