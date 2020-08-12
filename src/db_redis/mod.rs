use std::sync::{Mutex, Arc};
use redis::{ConnectionInfo, Msg, Client, ConnectionLike};
use redis::cluster::ClusterClient;
use std::sync::mpsc::Receiver;
use std::str::FromStr;
use redis::aio::PubSub;
use std::net::TcpStream;

static REDIS_SUB_POOL_NAME: &'static str = "redis_sub";

pub struct RedisPool {
    pub db_redis: Vec<ClusterClient>,
    pub url_list: Vec<String>,
    pub mutex: Mutex<i32>,
}

static mut el: *mut RedisPool = 0 as *mut _;

impl RedisPool {
    pub fn new() -> RedisPool {
        RedisPool {
            db_redis: Vec::new(),
            url_list: Vec::new(),
            mutex: Mutex::new(0),
        }
    }

    pub fn instance() -> &'static mut RedisPool {
        unsafe {
            if el == 0 as *mut _ {
                el = Box::into_raw(Box::new(RedisPool::new()));
            }

            &mut *el
        }
    }

    fn init_connection(&self) -> Option<ClusterClient> {
        let mut cluster = ok_or!(ClusterClient::open(self.url_list.clone()), return None);
        Some(cluster)
    }

    fn set_url_list(&mut self, url_list: Vec<String>) -> bool {
        self.url_list = url_list;
        true
    }

    pub fn get_redis_connection(&mut self) -> Option<ClusterClient> {
        let _guard = self.mutex.lock().unwrap();
        if self.db_redis.is_empty() {
            return self.init_connection();
        }
        self.db_redis.pop()
    }

    pub fn release_redis_connection(&mut self, client: ClusterClient) {
        let _guard = self.mutex.lock().unwrap();
        self.db_redis.push(client);
    }
}