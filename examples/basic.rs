extern crate redis;
extern crate redis_cluster;

use redis::Commands;
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

    let _: () = clus.set("hey", "there").unwrap();
    let res: String = clus.get("hey").unwrap();
    println!("{:?}", res);
    assert_eq!(res, "there");
}
