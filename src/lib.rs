extern crate rand;
extern crate redis;

mod cmd;
mod crc16;
mod slots;

use cmd::ClusterCmd;
use slots::get_slots;
use crc16::key_hash_slot;
use std::collections::HashMap;
use rand::{thread_rng, sample};
use redis::{Connection, RedisResult, FromRedisValue, Client, ConnectionLike, Commands, Value};

const TTL: usize = 16;

fn connect(info: &str) -> Connection {
    let client = Client::open(info).unwrap();
    client.get_connection().unwrap()
}

pub struct Cluster {
    conns: HashMap<String, Connection>,
    slots: HashMap<u16, String>,
}

impl Cluster {
    pub fn new(startup_nodes: Vec<&str>) -> Cluster {
        let mut slots = HashMap::new();
        let mut conns = HashMap::new();

        for info in startup_nodes {
            let conn = connect(info);
            for slot_data in get_slots(&conn) {
                for (slot, addr) in slot_data.nodes() {
                    slots.insert(slot, addr);
                }
            }
            conns.insert(info.to_string(), conn);

            // this loop can terminate if the first node replies
            break;
        }

        Cluster {
            conns: conns,
            slots: slots,
        }
    }

    fn get_connection_by_slot(&self, slot: u16) -> Option<&Connection> {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        match addr {
            Some(ref addr) => {
                if self.conns.contains_key(addr) {
                    Some(self.conns.get(addr).unwrap())
                } else {
                    None
                }
            }

            // just return a random connection
            None => Some(self.get_random_connection()),
        }
    }

    fn get_or_create_connection_by_slot(&mut self, slot: u16) -> &Connection {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        match addr {
            Some(ref addr) => {
                if self.conns.contains_key(addr) {
                    self.conns.get(addr).unwrap()
                } else {
                    // create the connection
                    let conn = connect(addr);
                    self.conns.insert(addr.to_string(), conn);
                    self.conns.get(addr).unwrap()
                }
            }

            // just return a random connection
            None => self.get_random_connection(),
        }
    }

    fn get_random_connection(&self) -> &Connection {
        let mut rng = thread_rng();
        // TODO can shuffle Rng::shuffle
        // and cmd.arg("PING").execute(&conn)
        // to check if the connection is still live
        // see: <https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb#L174>
        sample(&mut rng, self.conns.values(), 1).first().unwrap()
    }

    pub fn send_cluster_command<T: FromRedisValue>(&mut self, cmd: &ClusterCmd) -> RedisResult<T> {
        let mut try_random_node = false;
        for _ in 0..TTL {
            let slot = match cmd.slot() {
                Some(slot) => slot,
                None => panic!("No way to dispatch this command to Redis Cluster"),
            };
            let conn = if try_random_node {
                try_random_node = false;
                self.get_random_connection()
            } else {
                self.get_or_create_connection_by_slot(slot)
            };
            match cmd.query(conn) {
                Ok(res) => return Ok(res),
                Err(_) => {
                    // TODO handle MOVE/ASK errors,
                    // refer to <https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb#L245>
                    try_random_node = true;
                }
            }
        }
        panic!("Too many redirections");
    }
}

impl ConnectionLike for Cluster {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        let slot = key_hash_slot(cmd);
        // TODO we dont have mutable access to self so we can't get_or_create_connection_by_slot...
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_command(cmd)
    }

    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>> {
        let slot = key_hash_slot(cmd);
        // TODO we dont have mutable access to self so we can't get_or_create_connection_by_slot...
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Commands for Cluster {}
