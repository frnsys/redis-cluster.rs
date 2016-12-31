extern crate rand;
extern crate redis;

mod crc16;

use crc16::key_hash_slot;
use std::collections::HashMap;
use rand::{thread_rng, sample};
use redis::{Connection, Pipeline, RedisResult, ErrorKind, FromRedisValue, Cmd, Client};

const TTL: usize = 16;

pub struct RedisCluster {
    conns: HashMap<String, Connection>,
    slots: HashMap<u16, String>,
}

impl RedisCluster {
    pub fn new() -> RedisCluster {
        RedisCluster {
            conns: HashMap::new(),
            slots: HashMap::new(),
        }
    }

    pub fn add(&mut self, info: &str) -> RedisResult<()> {
        let conn = self.connect(info);
        self.conns.insert(info.to_string(), conn);
        Ok(())
    }

    fn connect(&self, info: &str) -> Connection {
        let client = Client::open(info).unwrap();
        client.get_connection().unwrap()
    }

    fn get_connection_by_slot(&mut self, slot: u16) -> RedisResult<&Connection> {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        match addr {
            Some(ref addr) => {
                if self.conns.contains_key(addr) {
                    Ok(self.conns.get(addr).unwrap())
                } else {
                    // create the connection
                    let conn = self.connect(addr);
                    self.conns.insert(addr.to_string(), conn);
                    Ok(self.conns.get(addr).unwrap())
                }
            }

            // just return a random connection
            None => Ok(self.get_random_connection()),
        }
    }

    fn get_random_connection(&self) -> &Connection {
        let mut rng = thread_rng();
        sample(&mut rng, self.conns.values(), 1).first().unwrap()
    }

    pub fn send_cluster_command<T: FromRedisValue>(&mut self, cmd: &Cmd) -> RedisResult<T> {
        // TODO
        // to get a slot for a command, we need access to cmd.args, which is a private field.
        // so...for now just getting a random connection....
        // refer to <https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb#L220>
        // and <https://github.com/tickbh/td_rredis/blob/a9330138e35188603bdbac55ffb846c60919d577/src/cmd.rs#L278>
        // this is a really basic implementation. see the referenced links above.
        for _ in 0..TTL {
            let conn = self.get_random_connection();
            // TODO better error handling,
            // refer to <https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb#L245>
            match cmd.query(conn) {
                Ok(res) => return Ok(res),
                Err(_) => continue,
            }
        }
        panic!("Too many redirections");
    }

    pub fn send_cluster_pipeline<T: FromRedisValue>(&mut self, pipe: &Pipeline) -> RedisResult<T> {
        // See above TODO
        for _ in 0..TTL {
            let conn = self.get_random_connection();
            // See above TODO
            match pipe.query(conn) {
                Ok(res) => return Ok(res),
                Err(_) => continue,
            }
        }
        panic!("Too many redirections");
    }
}
