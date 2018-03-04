use std::net::SocketAddr;
use redis::{Connection, Cmd, ConnectionLike, RedisResult, Value};

pub const SLOT_SIZE: usize = 16384;

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

pub fn get_slots(conn: &Connection) -> RedisResult<Vec<SlotInfo>> {
    // manually handle the parsing of the response
    // since the built-in parser doesn't handle mixed types well
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let pcmd = cmd.get_packed_command();
    let val = conn.req_packed_command(&pcmd)?;

    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = val {
        let mut iter = items.into_iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start_slot = if let Value::Int(start_slot) = item[0] {
                start_slot as u16
            } else {
                continue;
            };

            let end_slot = if let Value::Int(end_slot) = item[1] {
                end_slot as u16
            } else {
                continue;
            };

            let mut nodes: Vec<SocketAddr> = item.into_iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };

                        let port = if let Value::Int(port) = node[1] {
                            port
                        } else {
                            return None;
                        };
                        Some(format!("{}:{}", ip, port).parse().unwrap())
                    } else {
                        None
                    }
                })
                .collect();

            let replicas = nodes.split_off(1);
            let slot_info = SlotInfo {
                start_slot,
                end_slot,
                master: nodes[0],
                replicas
            };
            result.push(slot_info);
        }
    }

    Ok(result)
}
