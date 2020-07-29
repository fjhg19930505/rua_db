use mysql::{Conn as MysqlConn, Result as MysqlResult, QueryResult, Opts, Value};
use time;
use crate::NetResult;
use crate::db::db_trait::DbTrait;
use crate::db_trait::DbTrait;
use rua_net_mgr::{NetMsg, NetResult, NetConfig};
use mysql::prelude::Queryable;
use std::collections::HashMap;
use rua_value_list::{ValueType, ObjId, Put};

pub struct DbMysql {
    pub conn: MysqlConn,
    pub last_insert_id: u64,
    pub affected_rows: u64,
    pub error: Option<mysql::Error>,
    pub is_connect: bool,
    pub last_use_time: f64,
}

impl DbMysql {
    pub fn new(conn: MysqlConn) -> DbMysql {
        DbMysql {
            conn,
            last_insert_id: 0,
            affected_rows: 0,
            error: None,
            is_connect: true,
            last_use_time: (time::OffsetDateTime::now_utc() - time::OffsetDateTime::unix_epoch()).as_seconds_f64(),
        }
    }

    pub fn is_io_error<T>(value: &MysqlResult<T>) -> bool {
        match value {
            &Err(ref val) => {
                match val {
                    &mysql::Error::IoError(_) => return true,
                    _ => (),
                }
            }
            _ => (),
        }
        false
    }

    pub fn check_connect(&mut self) -> NetResult<()> {
        if !self.conn.ping() {
            self.is_connect = false;
            unwrap_or!(self.conn.reset().ok(), fail!((ErrorKind::IoError, "connect db error!")));
        }
        Ok(())
    }

    pub fn from_url_basic(url: &str) -> Option<Opts> {
        let url = url::Url::parse(url).unwrap();
        if url.scheme() != "mysql" {
            return None;
        }

        let opts = Opts::from_url(url.as_ref()).unwrap();
        Some(opts)
    }
}

impl DbTrait for DbMysql {
    fn select(&mut self, sql_cmd: &str, msg: &mut NetMsg) -> NetResult<i32> {
        self.check_connect()?;
        let value = self.conn.query_iter(sql_cmd);

        let config = NetConfig::instance();
        let mut success: i32 = 0;
        match value {
            Ok(val) => {
                self.last_insert_id = val.last_insert_id().unwrap_or(0);
                self.affected_rows = val.affected_rows();

                let mut array = vec![];
                let mut columns = HashMap::new();
                for (i, column) in val.columns().as_ref().iter().enumerate() {
                    columns.insert(String::from_utf8_lossy(&column.org_name_ref()[..]).to_string(), i);
                }

                for (_, row) in val.enumerate() {
                    let mut hash = HashMap::<String, Value>::new();
                    let mut row = row.unwrap();

                    for (name, idx) in &columns {
                        let field = unwrap_or!(config.get_field_by_name(name), continue);
                        let fix_value = match row.take(*idx) {
                            Some(row_val) => {
                                match row_val {
                                    mysql::Value::NULL => continue,
                                    mysql::Value::Bytes(sub_val) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_STR => {
                                                Value::from(unwrap_or!(String::from_utf8(sub_val).ok(), continue))
                                            }
                                            rua_value_list::STR_TYPE_OBJ => {
                                                let str = unwrap_or!(String::from_utf8(sub_val).ok(), continue);
                                                Value::from(ObjId::from(str))
                                            }
                                            _ =>continue,
                                        }
                                        Value::from(unwrap_or!(String::from_utf))
                                    }
                                    mysql::Value::Int(sub_val) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_U8 => Value::from(sub_val as u8),
                                            rua_value_list::STR_TYPE_U16 => Value::from(sub_val as u16),
                                            rua_value_list::STR_TYPE_U32 => Value::from(sub_val as u32),
                                            rua_value_list::STR_TYPE_U64 => Value::from(sub_val as u64),
                                            rua_value_list::STR_TYPE_U128 => Value::from(sub_val as u128),
                                            rua_value_list::STR_TYPE_I8 => Value::from(sub_val as i8),
                                            rua_value_list::STR_TYPE_I16 => Value::from(sub_val as i16),
                                            rua_value_list::STR_TYPE_I32 => Value::from(sub_val as i32),
                                            rua_value_list::STR_TYPE_I64 => Value::from(sub_val as i64),
                                            rua_value_list::STR_TYPE_I128 => Value::from(sub_val as i128),
                                            rua_value_list::STR_TYPE_F32 => Value::from(sub_val as f32),
                                            rua_value_list::STR_TYPE_F64 => Value::from(sub_val as f64),
                                        }
                                    }
                                    mysql::Value::UInt(sub_val) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_U8 => Value::from(sub_val as u8),
                                            rua_value_list::STR_TYPE_U16 => Value::from(sub_val as u16),
                                            rua_value_list::STR_TYPE_U32 => Value::from(sub_val as u32),
                                            rua_value_list::STR_TYPE_U64 => Value::from(sub_val as u64),
                                            rua_value_list::STR_TYPE_U128 => Value::from(sub_val as u128),
                                            rua_value_list::STR_TYPE_I8 => Value::from(sub_val as i8),
                                            rua_value_list::STR_TYPE_I16 => Value::from(sub_val as i16),
                                            rua_value_list::STR_TYPE_I32 => Value::from(sub_val as i32),
                                            rua_value_list::STR_TYPE_I64 => Value::from(sub_val as i64),
                                            rua_value_list::STR_TYPE_I128 => Value::from(sub_val as i128),
                                            rua_value_list::STR_TYPE_F32 => Value::from(sub_val as f32),
                                            rua_value_list::STR_TYPE_F64 => Value::from(sub_val as f64),
                                        }
                                    }
                                    mysql::Value::Float(sub_val) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_U8 => Value::from(sub_val as u8),
                                            rua_value_list::STR_TYPE_U16 => Value::from(sub_val as u16),
                                            rua_value_list::STR_TYPE_U32 => Value::from(sub_val as u32),
                                            rua_value_list::STR_TYPE_U64 => Value::from(sub_val as u64),
                                            rua_value_list::STR_TYPE_U128 => Value::from(sub_val as u128),
                                            rua_value_list::STR_TYPE_I8 => Value::from(sub_val as i8),
                                            rua_value_list::STR_TYPE_I16 => Value::from(sub_val as i16),
                                            rua_value_list::STR_TYPE_I32 => Value::from(sub_val as i32),
                                            rua_value_list::STR_TYPE_I64 => Value::from(sub_val as i64),
                                            rua_value_list::STR_TYPE_I128 => Value::from(sub_val as i128),
                                            rua_value_list::STR_TYPE_F32 => Value::from(sub_val as f32),
                                            rua_value_list::STR_TYPE_F64 => Value::from(sub_val as f64),
                                        }
                                    }
                                    mysql::Value::Date(_, _, _, _, _, _, _) => {
                                        match &*field.pattern {
                                            td_rp::STR_TYPE_U32 => {
                                                let timespec =
                                                    mysql::from_value::<Timespec>(row_val);
                                                Value::from(timespec.sec as u32)
                                            }
                                            _ => continue,
                                        }
                                    }
                                    _ => continue,
                                }
                            }
                            None => continue,
                        };

                        msg.get_var_list().put(name.clone()).put(fix_value);
                    }
                }
                self.error = None;
            }
            Err(val) => {
                match val {
                    mysql::Error::MySqlError(ref val) => success = val.code as i32,
                    _ => success = -1,
                }
                self.error = Some(val);
            }

        }
        Ok(success)
    }

    fn execute(&mut self, sql_cmd: &str) -> NetResult<i32> {
        unimplemented!()
    }

    fn insert(&mut self, sql_cmd: &str, msg: &mut _) -> _ {
        unimplemented!()
    }

    fn begin_transaction(&mut self) -> _ {
        unimplemented!()
    }

    fn commit_transaction(&mut self) -> _ {
        unimplemented!()
    }

    fn rollback_transaction(&mut self) -> _ {
        unimplemented!()
    }

    fn get_last_insert_id(&mut self) -> u64 {
        unimplemented!()
    }

    fn get_affected_rows(&mut self) -> u64 {
        unimplemented!()
    }

    fn get_character_set(&mut self) -> u8 {
        unimplemented!()
    }

    fn is_connected(&self) -> bool {
        unimplemented!()
    }

    fn get_error_code(&mut self) -> i32 {
        unimplemented!()
    }

    fn get_error_str(&mut self) -> Option<String> {
        unimplemented!()
    }
}
