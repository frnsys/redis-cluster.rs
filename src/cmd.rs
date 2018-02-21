use crc16::*;
use redis::{Cmd, Connection, ToRedisArgs, FromRedisValue, RedisResult};
use slots::SLOT_SIZE;

/// Redis::Cmd's `args` field is private,
/// but we need it to determine a slot from the command.
/// So this is a simple wrapper around Redis::Cmd that keeps
/// track of the args.
#[deprecated]
pub struct ClusterCmd {
    cmd: Cmd
}

impl ClusterCmd {
    pub fn new() -> ClusterCmd {
        ClusterCmd {
            cmd: Cmd::new()
        }
    }

    /// Add an arg to the command.
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut ClusterCmd {
        self.cmd.arg(arg);
        self
    }

    /// Execute the command, returning the result.
    pub fn query<T: FromRedisValue>(&self, conn: &Connection) -> RedisResult<T> {
        self.cmd.query(conn)
    }

    /// Get the packed command as a byte vector.
    pub fn get_packed_command(&self) -> Vec<u8> {
        self.cmd.get_packed_command()
    }
}

pub fn slot_for_packed_command(cmd: &[u8]) -> Option<u16> {
    let args = unpack_command(cmd);
    if args.len() > 1 {
        Some(State::<XMODEM>::calculate(&args[1]) % SLOT_SIZE as u16)
    } else {
        None
    }
}

/// `redis-rs` passes packed commands (as a u8 slice)
/// to the methods of the Commands trait
/// we need to "unpack" the command into the
/// original arguments to properly compute
/// the command's slot.
/// This is pretty messy/can probably be better
fn unpack_command(cmd: &[u8]) -> Vec<Vec<u8>> {
    let mut arg: Vec<u8> = Vec::new();
    let mut args: Vec<Vec<u8>> = Vec::new();

    // first 4 are some leading info ('*', len of args, '\r', '\n')
    // the next 4 precede the first arg
    // see: <https://github.com/mitsuhiko/redis-rs/blob/master/src/cmd.rs#L85>
    let mut iter = cmd.iter().skip(2).peekable();

    'outer: loop {
        let b = *iter.next().unwrap();

        // args are separated by 13, 10
        if b == 13 && iter.peek().unwrap() == &&10 {
            if arg.len() > 0 {
                args.push(arg.clone());
                arg.clear();
            }

            // consume the next item (10)
            iter.next();

            // then, if there are more args, there should be a 36
            // if there's nothing, we're done
            match iter.next() {
                Some(_) => (),
                None => break 'outer,
            };
            // then the length of the args (which in theory can be any length)
            // then another 13, 10
            'inner: loop {
                let b = *iter.next().unwrap();
                if b == 13 && iter.peek().unwrap() == &&10 {
                    iter.next();
                    break 'inner;
                }
            }
        } else {
            arg.push(b);
        }
    }
    args
}
