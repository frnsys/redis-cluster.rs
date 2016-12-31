use crc16::key_hash_slot;
use redis::{Cmd, Connection, ToRedisArgs, FromRedisValue, RedisResult};

/// Redis::Cmd's `args` field is private,
/// but we need it to determine a slot from the command.
/// So this is a simple wrapper around Redis::Cmd that keeps
/// track of the args.
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

    /// Add an arg to the command.
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut ClusterCmd {
        for item in arg.to_redis_args().into_iter() {
            self.args.push(item);
        }
        self.cmd.arg(arg);
        self
    }

    /// Execute the command, returning the result.
    pub fn query<T: FromRedisValue>(&self, conn: &Connection) -> RedisResult<T> {
        self.cmd.query(conn)
    }

    /// Get the slot for this command.
    pub fn slot(&self) -> Option<u16> {
        if self.args.len() > 1 {
            Some(key_hash_slot(self.args[1].as_slice()))
        } else {
            None
        }
    }
}
