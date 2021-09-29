//Libraries
use std::io::{Error, ErrorKind};
use std::sync::{Arc, RwLock};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper, ZkState, ZkError};

//The delimiting character separating the address and name in the znode data
static ZNODE_DELIM: &str = "\n";
//The znode name prefix for all replicas
static ZNODE_PREFIX: &str = "replica-";
//The length of the sequence number ZooKeeper adds to znodes
static SEQUENCE_LEN: u32 = 10;

//Required for creating a connection, but not used
struct NoopWatcher;
impl Watcher for NoopWatcher {
    fn handle(&self, _event: WatchedEvent) {}
}

//Returns the sequence number of a replica znode
pub fn get_replica_id(znode: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let mut znode_str = znode.to_string();

    //Check that the znode is long enough to contain a sequence number
    if znode_str.len() <= (SEQUENCE_LEN as usize) {
        return Err(Error::new(ErrorKind::InvalidInput,
            format!("The znode '{}' is missing a sequence number", znode_str)).into())
    }

    //Find the starting posiiton of the znode sequence
    let split_point = znode_str.len() - (SEQUENCE_LEN as usize);
    let id_string = znode_str.split_off(split_point);
    let replica_id = id_string.parse::<u32>();

    match replica_id {
        Ok(id) => Ok(id),
        _ => return Err(Error::new(ErrorKind::InvalidInput,
            format!("znode sequence '{}' is formatted incorrectly", id_string)).into())
    }
}

//Takes the a znode data string and returns the address
pub fn parse_znode_addr(znode_data: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut znode_data_str = znode_data.to_string();
    let result = znode_data_str.find(ZNODE_DELIM);

    match result {
        Some(position) => {
            if position == 0 {
                return Err(Error::new(ErrorKind::InvalidData,
                    format!("znode data is missing an address")).into())
            }
        },
        None => return Err(Error::new(ErrorKind::InvalidData,
            format!("znode data '{}' is formatted incorrectly", znode_data)).into())
    }

    let delim_pos = result.unwrap();
    //Split the address into znode_data_str 
    let _ = znode_data_str.split_off(delim_pos);

    Ok(znode_data_str)
}

//Takes a znode path and returns the address
pub fn get_node_address(instance: Arc<RwLock<ZooKeeper>>, znode: &str) -> Result<String, Box<dyn std::error::Error>> {
    let instance_read = instance.read().unwrap();
    let result = instance_read.get_data(znode, false);
    drop(instance_read);

    match result {
        Ok(node_data) => {
            let data_str = String::from_utf8(node_data.0)?;
            let address = parse_znode_addr(&data_str)?;

            Ok(address)
        }
        _ => {
            Err(Error::new(ErrorKind::InvalidData,
                format!("Failed to get the data of znode: {}", znode)).into())
        }
    }
}

//Creates the path to a new replica znode
pub fn new_replica_path(base_path: &str) -> String {
    return format!("{}/{}", base_path, ZNODE_PREFIX);
}

pub fn format_znode_data(server_addr: &str, name: &str) -> String {
    return format!("{}{}{}", server_addr, ZNODE_DELIM, name);
}

pub fn get_neighbor_znodes(instance: Arc<RwLock<ZooKeeper>>, base_path: &str, replica_id: u32) ->
Result<(Option<String>, Option<String>), Box<dyn std::error::Error>> {
    //Get all children of the base node
    let instance_read = instance.read().unwrap();
    let result = instance_read.get_children(&base_path, false);
    drop(instance_read);

    //Handle a connection error
    match result {
        Ok(_) => (),
        Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children in znode: {}", &base_path)).into())
    };

    let replica_list = result.as_ref().unwrap();
    let last_index = replica_list.len() - 1;
    let mut replica_index = 0;

    for replica in replica_list.iter() {
        let curr_replica_id = get_replica_id(&replica)?;

        if curr_replica_id == replica_id {
            let mut head = None;
            let mut tail = None;

            //Set the predecessor and successor if they exit
            if replica_index != 0 {
                head = Some(replica_list[replica_index - 1].to_owned());
            }
            if replica_index != last_index {
                tail = Some(replica_list[replica_index + 1].to_owned());
            }

            return Ok((head, tail));
        }

        replica_index += 1;
    }

    //There is an issue with the replica list
    Err(Error::new(ErrorKind::InvalidData, format!("Invalid chain position: {}", replica_index)).into())
}

pub fn get_neighbor_addrs(instance: Arc<RwLock<ZooKeeper>>, base_path: &str, replica_id: u32) ->
Result<(Option<String>, Option<String>), Box<dyn std::error::Error>> {
    let (pred_znode, succ_znode) = get_neighbor_znodes(instance.clone(), &base_path.clone(), replica_id)?;

    //Predecessor
    let pred_addr = match pred_znode {
        Some(znode) => {
            let znode_full = format!("{}/{}", &base_path, znode);

            match get_node_address(instance.clone(), &znode_full) {
                Ok(addr) => Some(addr),
                Err(err) => return Err(err)
            }
        },
        None => None,
    };

    //Successor
    let succ_addr = match succ_znode {
        Some(znode) => {
            let znode_full = format!("{}/{}", &base_path, znode);

            match get_node_address(instance.clone(), &znode_full) {
                Ok(addr) => Some(addr),
                Err(err) => return Err(err)
            }
        },
        None => None,
    };

    Ok((pred_addr, succ_addr))
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
pub fn create_recursive(instance: Arc<RwLock<ZooKeeper>>, znode_path: &str, znode_data: &str, create_mode: CreateMode)
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

    let instance_read = instance.read().unwrap();

    //Create the first znode, if it doesn't exist
    match instance_read.exists(&first_znode, false) {
        Ok(Some(_)) => curr_path = first_znode,
        Ok(None) => curr_path = create(instance.clone(), &first_znode, znode_data, create_mode)?,
        Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
            format!("Unable to create the znode: {}", curr_path)).into())
    }

    //Iteratively create the remaining znodes
    for znode in path_split {
        //Construct the path for the next znode
        curr_path = format!("{}/{}", curr_path, znode);

        //Create the znode if it doesn't exist
        match instance_read.exists(&curr_path, false) {
            Ok(Some(_)) => {},
            Ok(None) => curr_path = create(instance.clone(), &curr_path, znode_data, create_mode)?,
            Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
                format!("Unable to create the znode: {}", curr_path)).into()),
        };

        match instance_read.exists(&curr_path, false) {
            Ok(Some(_)) => println!("{} exists", curr_path),
            Ok(None) => println!("{} exists", curr_path),
            Err(_) => println!("WTF bro"),
        }
    }

    Ok(curr_path)
}

//Create a single znode at an existing path
pub fn create(instance: Arc<RwLock<ZooKeeper>>, znode_path: &str, znode_data: &str, create_mode: CreateMode)
-> Result<String, Box<dyn std::error::Error>> {
    let mut instance_write = instance.write().unwrap();

    let create_result = instance_write.create(znode_path,
        znode_data.as_bytes().to_vec(),
        Acl::open_unsafe().clone(),
        create_mode);

    drop(instance_write);

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