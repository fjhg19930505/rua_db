use rusqlite;
use rusqlite::{Connection, NO_PARAMS};
use crate::db_trait::DbTrait;
use rua_net_mgr::{NetMsg, NetResult, NetConfig};

use time;
use std::collections::HashMap;
use rusqlite::types::Value;

use rua_value_list;
use rua_value_list::{ValueType, Put, ErrorKind};

static DB_RESULT_PROTO: &'static str = "msg_db_result";
static LAST_INSERT_ID: &'static str = "sys_last_insert_id";

pub struct DbSqlite {
    pub conn: Connection,
    pub last_insert_id: u64,
    pub affected_rows: u64,
    pub error: Option<rusqlite::Error>,
    pub is_connect: bool,
    pub last_use_time: f64,
}

impl DbSqlite {
    fn new(conn: Connection) -> DbSqlite {
        DbSqlite {
            conn,
            last_insert_id: 0,
            affected_rows: 0,
            error: None,
            is_connect: true,
            last_use_time: (time::OffsetDateTime::now_utc() - time::OffsetDateTime::unix_epoch()).as_seconds_f64(),
        }
    }

    pub fn check_connect(&mut self) -> NetResult<()> {
        Ok(())
    }
}

impl DbTrait for DbSqlite {
    fn select(&mut self, sql_cmd: &str, msg: &mut NetMsg) -> NetResult<i32> {
        self.check_connect()?;
        let config = NetConfig::instance();
        let mut success = 0;
        let mut statement = match self.conn.prepare(sql_cmd) {
            Ok(statement) => statement,
            Err(err) => {
                match &err {
                    &rusqlite::Error::SqliteFailure(err, _) => success = err.extended_code,
                    _ => (),
                }
                println!("err = {:?}", err);
                self.error = Some(err);
                return Ok(success);
            }
        };

        let mut column_names = vec![];
        {
            for v in statement.column_names() {
                column_names.push(v.to_string());
            }
        }

        match statement.query(NO_PARAMS) {
            Ok(mut rows) => {
                self.error = None;
                while let Ok(row) = rows.next() {
                    let row = unwrap_or!(row, continue);
                    for i in 0..row.column_count() {
                        let column_name = &column_names[i as usize];
                        let field = unwrap_or!(config.get_field_by_name(column_name), continue);
                        match rua_value_list::get_type_by_name(&*field.pattern) {
                            ValueType::ValueTypeU8 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as u8;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeI8 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as i8;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeU16 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as u16;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeI16 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as i16;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeU32 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as u32;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeI32 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as i32;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeU64 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as u64;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeI64 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as i64;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeU128 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as u128;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeI128 => {
                                let value = unwrap_or!(row.get::<_, i32>(i).ok(), continue) as i128;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeF32 => {
                                let value = unwrap_or!(row.get::<_, f64>(i).ok(), continue) as f32;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeF64 => {
                                let value = unwrap_or!(row.get::<_, f64>(i).ok(), continue) as f64;
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            ValueType::ValueTypeStr => {
                                let value: String = unwrap_or!(row.get::<_, String>(i).ok(), continue);
                                msg.get_var_list().put(column_name.clone()).put(value);
                            }
                            _ => continue,
                        }
                    }

                }
            }
            Err(err) => {
                match &err {
                    &rusqlite::Error::SqliteFailure(err, _) => success = err.extended_code,
                    _ => success = -1,
                }
                self.error = Some(err);
                return Ok(success);
            }
        }

        Ok(0)
    }

    fn execute(&mut self, sql_cmd: &str) -> NetResult<i32> {
        self.check_connect()?;
        let mut success = 0;
        match self.conn.execute(sql_cmd, NO_PARAMS) {
            Err(err) => {
                match &err {
                    &rusqlite::Error::SqliteFailure(err, _) => success = err.extended_code,
                    _ => success = -1,
                }
                self.error = Some(err);
                return Ok(success)
            },
            _ => {
                self.error = None;
            },
        }
        Ok(success)
    }

    fn insert(&mut self, sql_cmd: &str, msg: &mut NetMsg) -> NetResult<i32> {
        self.check_connect()?;
        let value = self.conn.execute(sql_cmd, NO_PARAMS);
        let mut success: i32 = 0;
        match value {
            Ok(_) => {
                msg.get_var_list().put(DB_RESULT_PROTO.to_string()).put(self.last_insert_id as u32);
                self.error = None;
            }
            Err(val) => {
                match &val {
                    &rusqlite::Error::SqliteFailure(err, _) => success = err.extended_code,
                    _ => success = -1,
                }
                self.error = Some(val);
            }
        }
        Ok(success)
    }

    fn begin_transaction(&mut self) -> NetResult<i32> {
        self.execute("START TRANSACTION")
    }

    fn commit_transaction(&mut self) -> NetResult<i32> {
        self.execute("COMMIT")
    }

    fn rollback_transaction(&mut self) -> NetResult<i32> {
        self.execute("ROLLBACK")
    }

    fn get_last_insert_id(&mut self) -> u64 {
        self.last_insert_id
    }

    fn get_affected_rows(&mut self) -> u64 {
        self.affected_rows
    }

    fn get_character_set(&mut self) -> u8 {
        0u8
    }

    fn is_connected(&self) -> bool {
        false
    }

    fn get_error_code(&mut self) -> i32 {
        0
    }

    fn get_error_str(&mut self) -> Option<String> {
        None
    }
}