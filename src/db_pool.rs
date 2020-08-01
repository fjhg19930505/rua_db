use std::collections::HashMap;
use crate::db_mysql::DbMysql;
use std::sync::Mutex;
use crate::db_sqlite::DbSqlite;

pub struct  DbPool {
    pub db_mysql: HashMap<String, Vec<DbMysql>>,
    pub db_sqlite: HashMap<String, Vec<DbSqlite>>,
    pub db_info: HashMap<String, String>,
    pub mutex: Mutex<i32>,
}

pub enum DbStruct {
    MySql(DbMysql),
    Sqlite(DbSqlite),
}

impl DbPool {
    pub fn new() -> DbPool {
        DbPool {
            db_mysql: HashMap::new(),
            db_sqlite: HashMap::new(),
            db_info: HashMap::new(),
            mutex: Mutex::new(0),
        }
    }
}