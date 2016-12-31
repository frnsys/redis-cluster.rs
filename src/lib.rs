extern crate rand;
extern crate redis;

mod crc16;

use crc16::key_hash_slot;
use std::collections::HashMap;
use rand::{thread_rng, sample};
use redis::{Connection, Pipeline, RedisResult, ErrorKind, FromRedisValue, Cmd, Client,
            ConnectionLike, Commands, Value};

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

        // TODO can't seem to figure out how to read these
        // mixed-type arrays...
        for info in startup_nodes {
            let conn = connect(info);
            conns.insert(info.to_string(), conn);
            //     let mut cmd = Cmd::new();
            //     cmd.arg("CLUSTER").arg("SLOTS");

            // let res = cmd.query::<Vec<Vec<Vec<String>>>>(&conn);
            // let res = cmd.query::<Vec<Vec<u8>>>(&conn);
            // println!("{:?}", res);
            // for slot in cmd.query::<Vec<String>>(&conn) {
            //     println!("{:?}", slot);
            // }
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


    fn get_connection_by_slot(&mut self, slot: u16) -> RedisResult<&Connection> {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        match addr {
            Some(ref addr) => {
                if self.conns.contains_key(addr) {
                    Ok(self.conns.get(addr).unwrap())
                } else {
                    // create the connection
                    let conn = connect(addr);
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

impl ConnectionLike for Cluster {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        // TODO again, this shouldn't be random...
        self.get_random_connection().req_packed_command(cmd)
    }

    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>> {
        // TODO again, this shouldn't be random...
        self.get_random_connection().req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Commands for Cluster {}
