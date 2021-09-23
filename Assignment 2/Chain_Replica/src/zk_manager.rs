//Libraries
use std::io::{Error, ErrorKind};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper, ZkState};

//Required for creating a connection, but not used
struct NoopWatcher;
impl Watcher for NoopWatcher {
    fn handle(&self, _event: WatchedEvent) {}
}

pub fn connect<Listener: Fn(ZkState) + Send + 'static>(host_list: &str, listener: Listener, timeout: u64)
-> Result<ZooKeeper, Box<dyn std::error::Error>> {
    //Connecting to ZooKeeper
    let connect_result = ZooKeeper::connect(
        host_list,
        std::time::Duration::from_secs(timeout),
        NoopWatcher);

    //Handle a connection error
    match connect_result {
        Ok(_) => println!("Connecting to ZooKeeper host: '{}'...", host_list),
        Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
            format!("The host list '{}' is not valid.", host_list)).into())
    };

    let instance = connect_result.unwrap();
    instance.add_listener(listener);

    Ok(instance)
}

pub fn create(instance: &mut ZooKeeper, znode_path: &str, znode_data: &str, create_mode: CreateMode)
-> Result<String, Box<dyn std::error::Error>> {
    let create_result = instance.create(znode_path,
        znode_data.as_bytes().to_vec(),
        Acl::open_unsafe().clone(),
        create_mode);

    //Handle a creation error
    match create_result {
        Ok(_) => (),
        Err(_) => return Err(Error::new(ErrorKind::InvalidData,
            format!("Unable to create the znode: {}", znode_path)).into())
    };

    //Unwrap the full znode with the sequence number
    let znode = create_result.unwrap();

    println!("Successfully created zNode: {}", znode);

    Ok(znode)
}