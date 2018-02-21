extern crate crc16;
extern crate rand;
extern crate redis;

mod cmd;
mod slots;

pub use cmd::{ClusterCmd, slot_for_packed_command};
use slots::get_slots;
use std::collections::HashMap;
use rand::{thread_rng, sample};
use redis::{Connection, RedisResult, FromRedisValue, Client, ConnectionLike, Commands, Value, Cmd,
            ErrorKind};

const TTL: usize = 16;

fn connect(info: &str) -> Connection {
    let client = Client::open(info).unwrap();
    client.get_connection().unwrap()
}

fn check_connection(conn: &Connection) -> bool {
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    match cmd.query::<String>(conn) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub struct Cluster {
    startup_nodes: Vec<String>,
    conns: HashMap<String, Connection>,
    slots: HashMap<u16, String>,
    needs_refresh: bool,
}

impl Cluster {
    pub fn new(startup_nodes: Vec<&str>) -> Cluster {
        let mut conns = HashMap::new();
        let nodes = startup_nodes.iter().map(|s| s.to_string()).collect();
        for info in startup_nodes {
            let conn = connect(info);
            conns.insert(info.to_string(), conn);
        }

        let mut clus = Cluster {
            conns: conns,
            slots: HashMap::new(),
            needs_refresh: false,
            startup_nodes: nodes,
        };
        clus.refresh_slots();
        clus
    }

    /// Query a node to discover slot-> master mappings.
    fn refresh_slots(&mut self) {
        for conn in self.conns.values() {
            for slot_data in get_slots(&conn) {
                for (slot, addr) in slot_data.nodes() {
                    self.slots.insert(slot, addr);
                }
            }
            // this loop can terminate if the first node replies
            break;
        }
        self.refresh_conns();
        self.needs_refresh = false;
    }

    /// Remove dead connections and connect to new nodes if necessary
    fn refresh_conns(&mut self) {
        for addr in self.slots.values() {
            if self.conns.contains_key(addr) {
                let ok = {
                    let conn = self.conns.get(addr).unwrap();
                    check_connection(conn)
                };
                if !ok {
                    self.conns.remove(addr);
                }
            } else {
                let conn = connect(addr);
                self.conns.insert(addr.to_string(), conn);
            }
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
        sample(&mut rng, self.conns.values(), 1).first().unwrap()
    }

    pub fn send_command<T: FromRedisValue>(&mut self, cmd: &ClusterCmd) -> RedisResult<T> {
        if self.needs_refresh {
            self.refresh_slots();
        }
        let mut try_random_node = false;
        for _ in 0..TTL {
            let slot = match cmd.slot() {
                Some(slot) => slot,
                None => panic!("No way to dispatch this command to Redis Cluster"),
            };
            let res = {
                let conn = if try_random_node {
                    try_random_node = false;
                    self.get_random_connection()
                } else {
                    self.get_or_create_connection_by_slot(slot)
                };
                cmd.query(conn)
            };
            match res {
                Ok(res) => return Ok(res),
                Err(err) => {
                    if err.kind() == ErrorKind::ExtensionError &&
                       err.extension_error_code().unwrap() == "MOVED" {
                        self.needs_refresh = true;
                    }
                    try_random_node = true;
                }
            }
        }
        panic!("Too many redirections");
    }
}

impl ConnectionLike for Cluster {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        // TODO we dont have mutable access to self so we can't get_or_create_connection_by_slot...
        let slot = slot_for_packed_command(cmd).unwrap();
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_command(cmd)
    }

    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>> {
        // TODO we dont have mutable access to self so we can't get_or_create_connection_by_slot...
        let slot = slot_for_packed_command(cmd).unwrap();
        let conn = self.get_connection_by_slot(slot).unwrap();
        conn.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Commands for Cluster {}

impl Clone for Cluster {
    fn clone(&self) -> Cluster {
        let startup_nodes = self.startup_nodes.iter().map(|s| s.as_ref()).collect();
        Cluster::new(startup_nodes)
    }
}
