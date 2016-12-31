use std::str::from_utf8;
use std::net::SocketAddr;
use redis::{Connection, Cmd, ConnectionLike, Value};

#[derive(Debug)]
pub struct SlotInfo {
    start_slot: u16,
    end_slot: u16,
    master: SocketAddr,
    replicas: Vec<SocketAddr>,
}

impl SlotInfo {
    pub fn nodes(&self) -> Vec<(u16, String)> {
        (self.start_slot..self.end_slot)
            .map(|slot| (slot, format!("redis://{}", self.master.to_string())))
            .collect()
    }
}

pub fn get_slots(conn: &Connection) -> Vec<SlotInfo> {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");

    // manually handle the parsing of the response
    // since the built-in parser doesn't handle mixed types well
    let pcmd = cmd.get_packed_command();
    let val = conn.req_packed_command(&pcmd).unwrap();

    // this will look something like:
    // "10923:16383:127.0.0.1:7002:127.0.0.1:7005 5461:10922:127.0.0.1:7001:127.0.0.1:7004"
    // i.e. nodes are delimited by whitespace and their data is delimited by ":"
    let data = redis_value_to_strings(val, " ");
    data.split(" ")
        .map(|node_data| {
            // this is kinda nuts, there may be a nicer way of handling this
            let parts: Vec<&str> = node_data.split(":").collect();
            let (start_slot, parts) = parts.split_first().unwrap();
            let (end_slot, parts) = parts.split_first().unwrap();
            let (master_addr, parts) = parts.split_at(2);
            let replicas = parts.chunks(2)
                .map(|addr| addr.join(":").parse().unwrap())
                .collect();
            SlotInfo {
                start_slot: start_slot.parse::<u16>().unwrap(),
                end_slot: end_slot.parse::<u16>().unwrap(),
                master: master_addr.join(":").parse().unwrap(),
                replicas: replicas,
            }
        })
        .collect()
}

// take a redis response value and just dump it to a long string.
// we use this to manually parse out the cluster slot response,
// which has mixed types
fn redis_value_to_strings(val: Value, delim: &str) -> String {
    match val {
        Value::Bulk(vs) => {
            let mut parsed = Vec::new();
            for v in vs {
                parsed.push(redis_value_to_strings(v, ":"));
            }
            parsed.join(delim)
        }
        Value::Int(v) => v.to_string(),
        Value::Data(v) => from_utf8(v.as_slice()).unwrap().to_string(),
        _ => "".to_string(),
    }
}
