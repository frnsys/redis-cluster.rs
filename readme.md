Rust port of [redis-rb-cluster](https://github.com/antirez/redis-rb-cluster/blob/master/cluster.rb) for interfacing with a Redis Cluster.

## Example

A simple example is included in `examples/basic.rs`.

You need to spin up a Redis Cluster before running it. You can run `examples/cluster/start.sh` to start some Redis nodes to form the cluster. Then, to create the cluster:

    ./examples/cluster/redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005

Then run the example:

    cargo run --example basic