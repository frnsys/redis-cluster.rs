extern crate rand;
extern crate redis;

mod crc16;

use crc16::key_hash_slot;
use std::collections::HashMap;
use rand::{thread_rng, sample};
use redis::{Connection, Pipeline, RedisResult, ErrorKind, ToRedisArgs, FromRedisValue, Cmd,
            Client, ConnectionLike, Commands, Value};

const TTL: usize = 16;

fn connect(info: &str) -> Connection {
    let client = Client::open(info).unwrap();
    client.get_connection().unwrap()
}

fn get_slot_from_command(args: &Vec<Vec<u8>>) -> Option<u16> {
    if args.len() > 1 {
        Some(key_hash_slot(args[1].as_slice()))
    } else {
        None
    }
}

pub struct Cluster {
    conns: HashMap<String, Connection>,
    slots: HashMap<u16, String>,
}

pub struct ClusterCmd {
    cmd: Cmd,
    args: Vec<Vec<u8>>,
}

impl ClusterCmd {
    pub fn new() -> ClusterCmd {
        ClusterCmd {
            cmd: Cmd::new(),
            args: Vec::new(),
        }
    }

    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut ClusterCmd {
        for item in arg.to_redis_args().into_iter() {
            self.args.push(item);
        }
        self.cmd.arg(arg);
        self
    }
}

impl Cluster {
    pub fn new(startup_nodes: Vec<&str>) -> Cluster {
        let mut slots = HashMap::new();
        let mut conns = HashMap::new();

        // TODO can't seem to figure out how to read these
        // mixed-type arrays...
        for info in startup_nodes {
            let conn = connect(info);
            // let mut cmd = Cmd::new();
            // cmd.arg("CLUSTER").arg("SLOTS");
            // let res = cmd.query::<Vec<Vec<u8>>>(&conn);
            // let res = cmd.query::<Vec<Vec<Vec<String>>>>(&conn);
            // let res = cmd.query::<String>(&conn);
            // println!("{:?}", res);
            // for slot in cmd.query::<Vec<String>>(&conn) {
            //     println!("{:?}", slot);
            // }
            conns.insert(info.to_string(), conn);
            break; // TODO this loop can terminate if the first node replies
        }

        Cluster {
            conns: conns,
            slots: slots,
        }
    }

    pub fn add(&mut self, info: &str) -> RedisResult<()> {
        let conn = connect(info);
        self.conns.insert(info.to_string(), conn);
        Ok(())
    }

    fn get_connection_by_slot(&self, slot: u16) -> Option<&Connection> {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        match addr {
            Some(ref addr) => {
                if self.conns.contains_key(addr) {
                    Some(self.conns.get(addr).unwrap())
                } else {
                    // create the connection
                    // let conn = connect(addr);
                    // self.conns.insert(addr.to_string(), conn);
                    // Ok(self.conns.get(addr).unwrap())
                    None
                }
            }

            // just return a random connection
            None => Some(self.get_random_connection()),
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
            let slot = match get_slot_from_command(&cmd.args) {
                Some(slot) => slot,
                None => panic!("No way to dispatch this command to Redis Cluster"),
            };
            let conn = if try_random_node {
                try_random_node = false;
                self.get_random_connection()
            } else {
                self.get_connection_by_slot(slot).unwrap()
            };
            match cmd.cmd.query(conn) {
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
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_command(cmd)
    }

    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>> {
        let slot = key_hash_slot(cmd);
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Commands for Cluster {}
