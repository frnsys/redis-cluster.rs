Rust port of [redis-rb-cluster](https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb) for interfacing with a Redis Cluster.

Add to your `Cargo.toml`:

    [dependencies]
    redis-cluster = "0.1"

## Basic Usage

    extern crate redis_cluster;

    use redis_cluster::{Cluster, ClusterCmd};

    fn main() {
        let startup_nodes =
            vec!["redis://127.0.0.1:7000", "redis://127.0.0.1:7001", "redis://127.0.0.1:7002"];
        let mut clus = Cluster::new(startup_nodes);
        let mut cmd = ClusterCmd::new();
        cmd.arg("SET").arg("foo").arg("bar");
        let _: () = clus.send_command(&cmd).unwrap();

        let mut cmd = ClusterCmd::new();
        cmd.arg("GET").arg("foo");
        let res: String = clus.send_command(&cmd).unwrap();
        println!("{:?}", res);
        assert_eq!(res, "bar");
    }

## Example

A simple example is included in `examples/basic.rs`.

You need to spin up a Redis Cluster before running it. You can run `examples/cluster/start.sh` to start some Redis nodes to form the cluster. Then, to create the cluster:

    ./examples/cluster/redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005

Then run the example:

    cargo run --example basic

## Acknowledgements

The base for this code is [from here](https://github.com/tickbh/td_rredis).