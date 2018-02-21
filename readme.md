Rust port of [redis-rb-cluster](https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb) for interfacing with a Redis Cluster.

Add to your `Cargo.toml`:

    [dependencies]
    redis-cluster = "0.1"

## Basic Usage

    extern crate redis;
    extern crate redis_cluster;
    
    use redis::{cmd, Commands};
    use redis_cluster::Cluster;
    
    fn main() {
        let startup_nodes =
            vec!["redis://127.0.0.1:7000", "redis://127.0.0.1:7001", "redis://127.0.0.1:7002"];
    
        let clus = Cluster::new(startup_nodes).unwrap();
    
        let _: () = cmd("SET").arg("foo").arg("bar").query(&clus).unwrap();
        let res: String = cmd("GET").arg("foo").query(&clus).unwrap();
        println!("{:?}", res);
        assert_eq!(res, "bar");
    
        let _: () = clus.set("hey", "there").unwrap();
        let res: String = clus.get("hey").unwrap();
        println!("{:?}", res);
        assert_eq!(res, "there");
    }

## Example

A simple example is included in `examples/basic.rs`.

You need to spin up a Redis Cluster before running it. You can run `examples/cluster/start.sh` to start some Redis nodes to form the cluster. Then, to create the cluster:

    ./examples/cluster/redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005

Then run the example:

    cargo run --example basic

## TODO

- tests
- make it a drop-in replacement for `redis::Client`/`redis::Connection`

## Acknowledgements

The base for this code is [from here](https://github.com/tickbh/td_rredis).
