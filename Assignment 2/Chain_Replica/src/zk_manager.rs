//Libraries
use std::io::{Error, ErrorKind};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper, ZkState, ZkError};

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


//Recursively create the znodes in the path, using the same settings for each znode
pub fn create_recursive(instance: &mut ZooKeeper, znode_path: &str, znode_data: &str, create_mode: CreateMode)
-> Result<String, Box<dyn std::error::Error>> {
    //Check that the creation mode is valid
    match create_mode {
        CreateMode::Ephemeral | CreateMode::EphemeralSequential
        => return Err(Error::new(ErrorKind::InvalidInput,
            format!("Invalid path '{}'. Parent nodes cannot be ephemeral", znode_path)).into()),
        _ => {}
    }

    let mut path_split = znode_path.split("/").skip_while(|&s| s.is_empty());
    let mut curr_path = String::new();
    let first_znode = format!("/{}", path_split.next().unwrap());

    //Create the first znode, if it doesn't exist
    match instance.exists(&first_znode, false) {
        Ok(Some(_)) => curr_path = first_znode,
        Ok(None) => curr_path = create(instance, &first_znode, znode_data, create_mode)?,
        Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
            format!("Unable to create the znode: {}", curr_path)).into())
    }

    //Iteratively create the remaining znodes
    for znode in path_split {
        //Construct the path for the next znode
        curr_path = format!("{}/{}", curr_path, znode);

        //Create the znode if it doesn't exist
        match instance.exists(&curr_path, false) {
            Ok(Some(_)) => {},
            Ok(None) => curr_path = create(instance, &curr_path, znode_data, create_mode)?,
            Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
                format!("Unable to create the znode: {}", curr_path)).into()),
        };

        match instance.exists(&curr_path, false) {
            Ok(Some(_)) => println!("{} exists", curr_path),
            Ok(None) => println!("{} exists", curr_path),
            Err(_) => println!("WTF bro"),
        }
    }

    Ok(curr_path)
}

//Create a single znode at an existing path
pub fn create(instance: &mut ZooKeeper, znode_path: &str, znode_data: &str, create_mode: CreateMode)
-> Result<String, Box<dyn std::error::Error>> {
    let create_result = instance.create(znode_path,
        znode_data.as_bytes().to_vec(),
        Acl::open_unsafe().clone(),
        create_mode);

    //Handle a creation error
    match create_result {
        Ok(_) => (),
        Err(ZkError::NodeExists) =>
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("The znode '{}' already exists", znode_path)).into()),
        Err(err) => {
            eprintln!("Error is {} at path {}", err, znode_path);
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("Unable to create the znode: {}", znode_path)).into());
        }
    };

    //Unwrap the full znode path
    let znode = create_result.unwrap();

    Ok(znode)
}