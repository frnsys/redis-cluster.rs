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
