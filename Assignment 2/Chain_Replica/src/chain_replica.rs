/*!
 * Name:    Alex Wolski
 * Date:    September 22, 2021
 * Desc:    A chain replica node program managed by a ZooKeeper instance
 * 
 * Boilerplate code from the article: A beginner's guide to gRPC with Rust
 * https://dev.to/anshulgoyal15/a-beginners-guide-to-grpc-with-rust-3c7o
 * https://github.com/anshulrgoyal/rust-grpc-demo
!*/

mod grpc_services;
mod zk_manager;

mod replica_manager {
    use super::*;
    use std::io::{Error, ErrorKind};
    use std::ops::Range;
    use std::net::{SocketAddr};
    use std::sync::{Arc, RwLock};
    use tokio::runtime::Handle;
    use local_ip_address::{local_ip, list_afinet_netifas};
    use zookeeper::{CreateMode, ZooKeeper, ZkState, WatchedEvent};
    use grpc_services::{ReplicaData, ServerManager};


    pub struct Replica {
        socket: SocketAddr,
        base_path: String,
        replica_id: u32,
        shared_data: Arc<ReplicaData>,
        zk_instance: Arc<RwLock<ZooKeeper>>,
        server: Arc<RwLock<Option<ServerManager>>>,
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

        //Attempts to automatically find the local ip address
        //If the operation fails, prompt the user to choose an address
        fn get_local_ip() -> Result<String, Box<dyn std::error::Error>> {
            return match local_ip() {
                //Successfully found the local ip address
                Ok(ip) => {
                    let ip_string = ip.to_string();

                    //The local ip is invalid, prompt the user for a valid ip
                    if ip_string.starts_with("169.254") {
                        Replica::prompt_local_ip()
                    }
                    //If the local ip is valid
                    else {
                        Ok(ip_string)
                    }
                },
                //If the local_ip method fails, prompt the user to choose an address
                Err(_) => Replica::prompt_local_ip()
            };
        }


        pub fn new(host_list: &str, base_path: &str, server_port: &str, name: &str)
        -> Result<Replica, Box<dyn std::error::Error>> {
            //Construct the server host and port
            let local_ip = Replica::get_local_ip()?;
            let server_addr = format!("{}:{}", local_ip, server_port);
            let socket = server_addr.parse()?;

            //Construct the replica znode path (before the sequence number is added)
            let znode_path = zk_manager::new_replica_path(base_path);
            //Construct the contents of the znode
            let znode_data = zk_manager::format_znode_data(&server_addr, name);

            //Connect to the ZooKeeper host
            let mut instance = Arc::new(RwLock::new(zk_manager::connect(host_list, Replica::print_conn_state, 5)?));
            //Recursively create the znodes in the base path
            let _ = zk_manager::create_recursive(instance.clone(), base_path, "", CreateMode::Persistent)?;
            //Create the znode for this replica
            let znode = zk_manager::create(instance.clone(), &znode_path, &znode_data, CreateMode::EphemeralSequential)?;
            println!("Successfully created zNode: {}", znode);
            println!("Listening on: {}", server_addr);

            //Create the shared data for the servers
            let shared_data = Arc::new(ReplicaData::new(server_addr));
            //Instantiate the server
            let server = Arc::new(RwLock::new(Some(ServerManager::new(shared_data.clone(), Handle::current()))));

            Ok(Replica {
                socket: socket,
                base_path: base_path.to_string(),
                replica_id: zk_manager::get_replica_id(&znode)?,
                //Data shared by all services
                shared_data: shared_data.clone(),
                //ZooKeeper data
                zk_instance: instance.clone(),
                //Instantiate Servers
                server: server.clone(),
            })
        }

        pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            let (pred_znode, succ_znode) = zk_manager::get_neighbors(self.zk_instance.clone(), &self.base_path, self.replica_id)?;

            //Start the server
            let mut server_write = self.server.write().unwrap();
            let mut server_instance = server_write.as_mut().unwrap();
            server_instance.start(self.socket.clone(), pred_znode, succ_znode);
            drop(server_write);

            //Listen for ZooKeeper updates
            Replica::add_zk_watchers(self.server.clone(), self.zk_instance.clone(), self.base_path.clone(), self.replica_id);

            Ok(())
        }

        pub fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
            //Get ownership of the server
            let mut server_write = self.server.write().unwrap();
            let server_instance = server_write.take().unwrap();
            //Call its destructor
            server_instance.stop();

            Ok(())
        }

        fn add_zk_watchers(server: Arc<RwLock<Option<ServerManager>>>, instance: Arc<RwLock<ZooKeeper>>, base_path: String, replica_id: u32) {
            //Clone variabes to be moved into the watch method
            let instance_clone = instance.clone();
            let base_path_clone = base_path.clone();

            //Add the watch method
            let instance_read = instance_clone.read().unwrap();

            let _ = instance_read.get_children_w(&base_path_clone, move |event: WatchedEvent| {
                println!("Event: {:?}", event);
                //Update the replica with its predecessor and successor
                let _ = Replica::update(server.clone(), instance.clone(), base_path.clone(), replica_id);

                //Add another watch
                Replica::add_zk_watchers(server.clone(), instance.clone(), base_path.clone(), replica_id);
            });

            drop(instance_read);
        }

        //Check for changes in ZooKeeper
        fn update(server: Arc<RwLock<Option<ServerManager>>>, instance: Arc<RwLock<ZooKeeper>>, base_path: String, replica_id: u32) ->
        Result<(), Box<dyn std::error::Error>> {
            let (pred_znode, succ_znode) = zk_manager::get_neighbors(instance.clone(), &base_path, replica_id)?;

            //Predecessor
            let pred_addr = match pred_znode {
                Some(znode) => {
                    let znode_full = format!("{}/{}", &base_path, znode);

                    match zk_manager::get_node_address(instance.clone(), &znode_full) {
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

                    match zk_manager::get_node_address(instance.clone(), &znode_full) {
                        Ok(addr) => Some(addr),
                        Err(err) => return Err(err)
                    }
                },
                None => None,
            };
            
            let mut server_write = server.write().unwrap();
            let mut server_instance = server_write.as_mut().unwrap();
            server_instance.set_pred(pred_addr);
            server_instance.set_succ(succ_addr);

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

    println!("");
    let mut replica = replica_manager::Replica::new(&args[1], &args[2], server_port, name)?;
    replica.start()?;
    println!("");

    // To-do: Make graceful shutdown compatible with ZooKeeper
    match signal::ctrl_c().await {
        Ok(()) => {
            replica.stop()?;
            println!("Shutting replica down\n");
            Ok(())
        },

        Err(_) => { Err(Error::new(ErrorKind::Other, "Failed to listen for a shutdown signal").into()) },
    }
}