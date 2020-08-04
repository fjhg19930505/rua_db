use mysql::{Conn as MysqlConn, Result as MysqlResult, Opts, Value};
use time::{self};
use crate::db_trait::DbTrait;
use rua_net_mgr::{NetMsg, NetResult, NetConfig, ErrorKind};
use mysql::prelude::Queryable;
use std::collections::HashMap;
use rua_value_list::{ObjId, Put, VarList};
use num_traits::{NumCast, cast};

static DB_RESULT_PROTO: &'static str = "msg_db_result";
static LAST_INSERT_ID: &'static str = "sys_last_insert_id";

trait Fill<T> {
    fn fill_num_val(var_list: &mut VarList, val_type: &str, val: T);
}
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
            unwrap_or!(self.conn.reset().ok(),
                       fail!((ErrorKind::IoError, "reconnect db error")));
            self.is_connect = true;
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

                let mut columns = HashMap::new();
                for (i, column) in val.columns().as_ref().iter().enumerate() {
                    columns.insert(String::from_utf8_lossy(&column.org_name_ref()[..]).to_string(), i);
                }

                for (_, row) in val.enumerate() {
                    let mut row = row.unwrap();

                    for (name, idx) in &columns {
                        let field = unwrap_or!(config.get_field_by_name(name), continue);
                        match row.take(*idx) {
                            Some(row_val) => {
                                match row_val {
                                    mysql::Value::NULL => continue,
                                    mysql::Value::Bytes(sub_val) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_STR => {
                                                msg.get_var_list().put(name.clone()).put(unwrap_or!(String::from_utf8(sub_val).ok(), continue));
                                            }
                                            rua_value_list::STR_TYPE_OBJ => {
                                                let str = unwrap_or!(String::from_utf8(sub_val).ok(), continue);
                                                msg.get_var_list().put(ObjId::from(str));
                                            }
                                            _ =>continue,
                                        }
                                    }
                                    mysql::Value::Int(sub_val) => {
                                        msg.get_var_list().put(name.clone());
                                        fill_num_val(msg.get_var_list(), &*field.pattern, sub_val);
                                    }
                                    mysql::Value::UInt(sub_val) => {
                                        msg.get_var_list().put(name.clone());
                                        fill_num_val(msg.get_var_list(), &*field.pattern, sub_val);
                                    }
                                    mysql::Value::Float(sub_val) => {
                                        msg.get_var_list().put(name.clone());
                                        fill_num_val(msg.get_var_list(), &*field.pattern, sub_val);
                                    }
                                    mysql::Value::Date(_, _, _, _, _, _, _) => {
                                        match &*field.pattern {
                                            rua_value_list::STR_TYPE_U32 => {
                                                let timespec =
                                                    mysql::from_value::<f64>(row_val);
                                                msg.get_var_list().put(name.clone()).put(timespec as u32);
                                            }
                                            _ => continue,
                                        }
                                    }
                                    _ => continue,
                                }
                            }
                            None => continue,
                        };


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
        self.check_connect()?;
        let value = self.conn.query_iter(sql_cmd);
        let mut success: i32 = 0;
        match value {
            Ok(val) => {
                self.last_insert_id = val.last_insert_id().unwrap();
                self.affected_rows = val.affected_rows();
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

    fn insert(&mut self, sql_cmd: &str, msg: &mut NetMsg) -> NetResult<i32> {
        self.check_connect()?;
        let value = self.conn.query_iter(sql_cmd);
        let mut success: i32 = 0;
        match value {
            Ok(val) => {
                self.last_insert_id = val.last_insert_id().unwrap();
                self.affected_rows = val.affected_rows();
                let mut hash = HashMap::<String, Value>::new();
                hash.insert(LAST_INSERT_ID.to_string(), Value::from(self.last_insert_id as u32));
                msg.get_var_list().put(DB_RESULT_PROTO.to_string()).put(self.last_insert_id as u32);
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
        match self.error {
            Some(ref err) => {
                match *err {
                    mysql::Error::MySqlError(ref val) => {return val.code as i32;},
                    _ => {return -1;},
                }
            }
            None => 0,
        }
    }

    fn get_error_str(&mut self) -> Option<String> {
        match self.error {
            Some(ref err) => {
                match *err {
                    mysql::Error::MySqlError(ref val) => Some(val.message.clone()),
                    _ => Some(format!("{}", err)),
                }
            }
            None => None,
        }
    }
}


fn fill_num_val<T: NumCast>(var_list: &mut VarList, val_type: &str, val: T) {
    match &*val_type {
        rua_value_list::STR_TYPE_U8 => {
            let val: u8 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_U16 => {
            let val: u16 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_U32 => {
            let val: u32 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_U64 => {
            let val: u64 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_U128 => {
            let val: u128 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_I8 => {
            let val: i8 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_I16 => {
            let val: i16 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_I32 => {
            let val: i32 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_I64 => {
            let val: i64 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_I128 => {
            let val: i128 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_F32 => {
            let val: f32 = cast(val).unwrap();
            var_list.put(val);
        },
        rua_value_list::STR_TYPE_F64 => {
            let val: f64 = cast(val).unwrap();
            var_list.put(val);
        },
        _ => {
            return;
        }
    }
}



