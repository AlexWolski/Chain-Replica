/*!
 * Name:    Alex Wolski
 * Date:    September 22, 2021
 * Desc:    A chain replica node program managed by a ZooKeeper instance
 * 
 * Boilerplate code from the article: A beginner's guide to gRPC with Rust
 * https://dev.to/anshulgoyal15/a-beginners-guide-to-grpc-with-rust-3c7o
 * https://github.com/anshulrgoyal/rust-grpc-demo
!*/


mod grpc_manager;
mod zk_manager;

mod replica_manager {
    use super::*;
    use std::io::{Error, ErrorKind};
    use std::ops::Range;
    use std::sync::{Arc, RwLock};
    use std::collections::HashMap;
    use std::net::{SocketAddr};
    use local_ip_address::{local_ip, list_afinet_netifas};
    use zookeeper::{CreateMode, ZooKeeper, ZkState};
    use grpc_manager::{ReplicaData, GRpcServer, HeadServerManager, TailServerManager, ReplicaServerManager};

    //The delimiting character separating the address and name in the znode data
    static ZNODE_DELIM: &str = "\n";
    //The znode name prefix for all replicas
    static ZNODE_PREFIX: &str = "replica-";
    //The length of the sequence number ZooKeeper adds to znodes
    static SEQUENCE_LEN: u32 = 10;


    pub struct Replica {
        zk_instance: ZooKeeper,
        socket: SocketAddr,
        base_path: String,
        replica_id: u32,
        shared_data: Arc<ReplicaData>,
        head_server: HeadServerManager,
        tail_server: TailServerManager,
        replica_server: ReplicaServerManager,
    }

    impl Replica {
        //Prints the ZooKeeper connection state
        fn print_conn_state(state: ZkState) {
            match state {
                ZkState::Connected | ZkState::ConnectedReadOnly =>
                    println!("Connected to ZooKeeper host"),
                ZkState::Closed =>
                    println!("Disconnected from ZooKeeper host"),
                _ => (),
            }
        }

        //Prompts a user for an integer between the given values
        fn prompt_int(range: Range<i32>) -> Result<i32, Box<dyn std::error::Error>> {
            let stdin = std::io::stdin();

            loop {
                let mut input = String::new();
                let _ = stdin.read_line(&mut input)?;
                //Discard the newline
                input = input.replace(&['\n', '\r'][..], "");

                match input.parse::<i32>() {
                    Ok(integer) => {
                        if range.contains(&integer) {
                            return Ok(integer);
                        }
                        else {
                            println!("Enter a value between {} and {}.", range.start, range.end)
                        }
                    }
                    Err(_) => { println!("'{}' is not an integer. Try again.", input) }
                }
            }
        }

        //Prompts the user to choose a local ip address
        fn prompt_local_ip() -> Result<String, Box<dyn std::error::Error>> {
            //Get a vector of all network adapters on the machine
            let interfaces = list_afinet_netifas().unwrap();
            //Vector of valid ip addresses
            let mut addresses = Vec::new();

            //Find the valid addresses
            for (_, ip) in interfaces.iter() {
                if ip.is_ipv4() {
                    let ip_str = ip.to_string();

                    //See IBM documentation for private address ranges
                    //https://www.ibm.com/docs/en/networkmanager/4.2.0?topic=translation-private-address-ranges
                    if
                    //Class A
                    ip_str.starts_with("10.") |
                    //Class B
                    ip_str.starts_with("172.") |
                    //Class C
                    ip_str.starts_with("192.168.") {
                        addresses.push(ip_str);
                    }
                }
            }

            match addresses.len() {
                0 => Err(Error::new(ErrorKind::Other, "No valid local addresses found").into()),
                1 => Ok(addresses[0].clone()),
                len => {
                    println!("\nSelect an address to listen to:");
                    println!("-------------------------------");

                    let mut index = 0;
                    for ip in addresses.iter() {
                        println!("{}: {}", index, ip);
                        index += 1;
                    }

                    let selection_range = Range::<i32> {
                        start: 0,
                        end: len as i32
                    };

                    let selection = Replica::prompt_int(selection_range)? as usize;

                    Ok(addresses[selection].to_owned())
                },
            }
        }

        //Returns the sequence number of a replica znode
        fn get_replica_id(znode: &str) -> Result<u32, Box<dyn std::error::Error>> {
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
        fn parse_znode_addr(znode_data: &str) -> Result<String, Box<dyn std::error::Error>> {
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

            let delim_pos = result.unwrap() - 1;
            //Split the address into znode_data_str 
            let _ = znode_data_str.split_off(delim_pos);

            Ok(znode_data_str)
        }

        // Takes a znode path and returns the address
        fn get_node_address(&self, znode: &str) -> Result<String, Box<dyn std::error::Error>> {
                let result = self.zk_instance.get_data(znode, false);

                match result {
                    Ok(node_data) => {
                        let data_str = String::from_utf8(node_data.0)?;
                        let address = Replica::parse_znode_addr(&data_str)?;

                        Ok(address)
                    }
                    _ => {
                        Err(Error::new(ErrorKind::InvalidData,
                            format!("Failed to get the data of znode: {}", znode)).into())
                    }
                }
        }

        //Checks if this replica is the head, assuming ZooKeeper orders the znode list from newest to oldest
        fn is_head(&self) -> Result<bool, Box<dyn std::error::Error>> {
            //Get all children of the base node
            let result = self.zk_instance.get_children(&self.base_path, false);

            //Handle a connection error
            match result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children in znode: {}", &self.base_path)).into())
            };

            let replica_list = result.as_ref().unwrap();
            let first_replica_id = Replica::get_replica_id(&replica_list[0])?;

            if first_replica_id == self.replica_id {
                Ok(true)
            }
            else {
                Ok(false)
            }
        }
                //Checks if this replica is the tail, assuming ZooKeeper orders the znode list from newest to oldest
        fn is_tail(&self) -> Result<bool, Box<dyn std::error::Error>> {
            //Get all children of the base node
            let result = self.zk_instance.get_children(&self.base_path, false);

            //Handle a connection error
            match result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children in znode: {}", &self.base_path)).into())
            };

            let replica_list = result.as_ref().unwrap();
            let last_index = replica_list.len() - 1;
            let last_replica_id = Replica::get_replica_id(&replica_list[last_index])?;

            if last_replica_id == self.replica_id {
                Ok(true)
            }
            else {
                Ok(false)
            }
        }

        fn get_pred(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
            //Get all children of the base node
            let result = self.zk_instance.get_children(&self.base_path, false);

            //Handle a connection error
            match result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children in znode: {}", &self.base_path)).into())
            };

            let replica_list = result.as_ref().unwrap();
            let mut replica_index = 0;

            for replica in replica_list.iter() {
                let curr_replica_id = Replica::get_replica_id(&replica)?;

                if curr_replica_id == self.replica_id {
                    //The replica is the head
                    if replica_index == 0 {
                        return Ok(None);
                    }
                    //Return the predecessor
                    else {
                        return Ok(Some(replica_list[replica_index - 1].to_owned()));
                    }
                }

                replica_index += 1;
            }

            //There is an issue with the replica list
            Err(Error::new(ErrorKind::InvalidData, format!("Invalid chain position: {}", replica_index)).into())
        }


        fn get_succ(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
            //Get all children of the base node
            let result = self.zk_instance.get_children(&self.base_path, false);

            //Handle a connection error
            match result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children in znode: {}", &self.base_path)).into())
            };

            let replica_list = result.as_ref().unwrap();
            let last_index = replica_list.len() - 1;
            let mut replica_index = 0;

            for replica in replica_list.iter() {
                let curr_replica_id = Replica::get_replica_id(&replica)?;

                if curr_replica_id == self.replica_id {
                    //The replica is the tail
                    if replica_index == last_index {
                        return Ok(None);
                    }
                    //Return the predecessor
                    else {
                        return Ok(Some(replica_list[replica_index + 1].to_owned()));
                    }
                }

                replica_index += 1;
            }

            //There is an issue with the replica list
            Err(Error::new(ErrorKind::Other, format!("Invalid chain position: {}", replica_index)).into())
        }

        pub fn new(host_list: &str, base_path: &str, server_port: &str, name: &str)
        -> Result<Replica, Box<dyn std::error::Error>> {
            //If no local IP address can be found, prompt the user to choose an IP address
            let local_ip = match local_ip() {
                Ok(ip) => ip.to_string(),
                Err(_) => Replica::prompt_local_ip()?
            };

            //Construct the server host and port
            let server_addr = format!("{}:{}", local_ip, server_port);
            let socket = server_addr.parse()?;

            //Construct the replica znode path (before the sequence number is added)
            let znode_path = format!("{}/{}", base_path, ZNODE_PREFIX);
            //Construct the contents of the znode
            let znode_data = format!("{}{}{}", server_addr, ZNODE_DELIM, name);

            //Connect to the ZooKeeper host
            let mut instance = zk_manager::connect(host_list, Replica::print_conn_state, 5)?;
            //Recursively create the znodes in the base path
            let _ = zk_manager::create_recursive(&mut instance, base_path, "", CreateMode::Persistent)?;
            //Create the znode for this replica
            let znode = zk_manager::create(&mut instance, &znode_path, &znode_data, CreateMode::EphemeralSequential)?;
            println!("Successfully created zNode: {}", znode);
            println!("Listening on: {}", server_addr);

            //Create the shared data for the servers
            let shared_data = Arc::new(ReplicaData{
                database: Arc::new(RwLock::new(HashMap::<String, i32>::new())),
                sent: Arc::new(RwLock::new(Vec::<(String, i32, u32)>::new())),
                ack: Arc::new(RwLock::new(Vec::<u32>::new())),
                my_addr: server_addr,
                pred_addr: Arc::new(RwLock::new(Option::<String>::None)),
                succ_addr: Arc::new(RwLock::new(Option::<String>::None)),
                //Assume that every node is added to the end of the chain
                new_tail: Arc::new(RwLock::new(false)),
            });

            Ok(Replica{
                //ZooKeeper data
                zk_instance: instance,
                socket: socket,
                base_path: base_path.to_string(),
                replica_id: Replica::get_replica_id(&znode)?,
                //Data shared by all services
                shared_data: shared_data.clone(),
                //Instantiate Servers
                head_server: HeadServerManager::new(shared_data.clone())?,
                tail_server: TailServerManager::new(shared_data.clone())?,
                replica_server: ReplicaServerManager::new(shared_data.clone())?,
            })
        }

        pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            let mut pause_head = true;

            if self.is_head()? {
                pause_head = false;
            }

            //The replica service should always be active
            //Since replicas are added to the end of the chian, the rail service should be active
            //The head service should only be active when there are no other replicas
            self.replica_server.start(self.socket.clone(), false)?;
            self.tail_server.start(self.socket.clone(), false)?;
            self.head_server.start(self.socket.clone(), pause_head)?;

            Ok(())
        }

        pub fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
            self.replica_server.stop();
            self.tail_server.stop();
            self.head_server.stop();

            Ok(())
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //The port all services will run on
    let server_port = "50051";
    //The name used in the znode data
    let name = "Alex Wolski";

    use std::env;
    use std::io::{Error, ErrorKind};
    use tokio::signal;

    let args: Vec<String> = env::args().collect();

    if args.len() != 3
    {
        println!("Correct Usage: chain_replica.rs ZOOKEEPER_HOST_PORT_LIST base_path");
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid number of arguments").into());
    }

    let mut replica = replica_manager::Replica::new(&args[1], &args[2], server_port, name)?;
    replica.start()?;

    match signal::ctrl_c().await {
        Ok(()) => {
            replica.stop()?;
            println!("Shutting replica down\n");
            Ok(())
        },

        Err(_) => { Err(Error::new(ErrorKind::Other, "Failed to listen for a shutdown signal").into()) },
    }
}