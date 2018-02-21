extern crate crc16;
extern crate rand;
extern crate redis;

mod cmd;
mod slots;

pub use cmd::{ClusterCmd, slot_for_packed_command};
use slots::{get_slots, SLOT_SIZE};
use std::cell::RefCell;
use std::collections::HashMap;
use rand::thread_rng;
use rand::seq::sample_iter;
use redis::{Connection, IntoConnectionInfo, RedisResult, FromRedisValue, Client, ConnectionLike, Commands, Value, Cmd, ErrorKind};

const TTL: usize = 16;

fn connect<T: IntoConnectionInfo>(info: T) -> RedisResult<Connection> {
    let connection_info = info.into_connection_info()?;
    let client = Client::open(connection_info)?;
    client.get_connection()
}

fn check_connection(conn: &Connection) -> bool {
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    match cmd.query::<String>(conn) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn get_random_connection<'a>(conns: &'a HashMap<String, Connection>) -> &'a Connection {
    let mut rng = thread_rng();
    let samples = sample_iter(&mut rng, conns.values(), 1).ok().unwrap();
    samples.first().unwrap()
}

pub struct Cluster {
    startup_nodes: Vec<String>,
    conns: RefCell<HashMap<String, Connection>>,
    slots: RefCell<HashMap<u16, String>>
}

impl Cluster {
    pub fn new(startup_nodes: Vec<&str>) -> Cluster {
        let mut conns = HashMap::with_capacity(startup_nodes.len());
        let mut nodes = Vec::with_capacity(startup_nodes.len());

        for info in startup_nodes {
            let conn = connect(info).unwrap();
            conns.insert(info.to_string(), conn);
            nodes.push(info.to_string());
        }

        let clus = Cluster {
            conns: RefCell::new(conns),
            slots: RefCell::new(HashMap::with_capacity(SLOT_SIZE)),
            startup_nodes: nodes
        };
        clus.refresh_slots();
        clus
    }

    #[deprecated]
    pub fn send_command<T: FromRedisValue>(&self, cmd: &ClusterCmd) -> RedisResult<T> {
        let packed_command = cmd.get_packed_command();
        self.request(&packed_command, move |conn| cmd.query(conn))
    }

    pub fn check_connection(&self) -> bool {
        let conns = self.conns.borrow();
        for conn in conns.values() {
            if !check_connection(&conn) {
                return false;
            }
        }
        true
    }

    /// Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) {
        {
            let conns = self.conns.borrow();
            let mut slots = self.slots.borrow_mut();
            slots.clear();

            for conn in conns.values() {
                let slots_data = get_slots(&conn);
                for slot_data in slots_data {
                    for (slot, addr) in slot_data.nodes() {
                        slots.insert(slot, addr);
                    }
                }
                // this loop can terminate if the first node replies
                break;
            }
        }
        self.refresh_conns();
    }

    /// Remove dead connections and connect to new nodes if necessary
    fn refresh_conns(&self) {
        let slots = self.slots.borrow();
        let mut conns = self.conns.borrow_mut();
        let mut new_conns = HashMap::with_capacity(conns.len());

        for addr in slots.values() {
            if !new_conns.contains_key(addr) {
                if conns.contains_key(addr) {
                    let conn = conns.remove(addr).unwrap();
                    if check_connection(&conn) {
                        new_conns.insert(addr.to_string(), conn);
                        continue;
                    }
                }

                if let Ok(conn) = connect(addr.as_ref()) {
                    if check_connection(&conn) {
                        new_conns.insert(addr.to_string(), conn);
                    }
                }
            }
        }
        *conns = new_conns
    }

    fn get_or_create_connection_by_slot<'a>(&self, conns: &'a mut HashMap<String, Connection>, slot: u16) -> &'a Connection {
        let slots = self.slots.borrow();

        if let Some(addr) = slots.get(&slot) {
            if conns.contains_key(addr) {
                return conns.get(addr).unwrap();
            }

            // Create new connection.
            if let Ok(conn) = connect(addr.as_ref()) {
                if check_connection(&conn) {
                    return conns.entry(addr.to_string()).or_insert(conn);
                }
            }
        }

        // just return a random connection
        get_random_connection(conns)
    }

    fn request<T, F>(&self, cmd: &[u8], func: F) -> RedisResult<T>
        where F: Fn(&Connection) -> RedisResult<T> {

        let mut i = 0;
        let mut try_random_node = false;
        let slot = slot_for_packed_command(cmd);

        loop {
            let res = {
                let mut conns = self.conns.borrow_mut();
                let conn = if try_random_node || slot.is_none() {
                    try_random_node = false;
                    get_random_connection(&*conns)
                } else {
                    self.get_or_create_connection_by_slot(&mut *conns, slot.unwrap())
                };

                func(conn)
            };

            match res {
                Ok(res) => return Ok(res),
                Err(err) => {
                    i += 1;
                    if i >= TTL {
                        return Err(err);
                    }

                    if err.kind() == ErrorKind::ExtensionError &&
                        (err.extension_error_code().unwrap() == "MOVED" || err.extension_error_code().unwrap() == "ASK") {

                        self.refresh_slots()
                    } else {
                        try_random_node = true;
                    }
                }
            }
        }
    }
}

impl ConnectionLike for Cluster {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        self.request(cmd, move |conn| conn.req_packed_command(cmd))
    }

    fn req_packed_commands(&self, cmd: &[u8], offset: usize, count: usize) -> RedisResult<Vec<Value>> {
        self.request(cmd, move |conn| conn.req_packed_commands(cmd, offset, count))
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
