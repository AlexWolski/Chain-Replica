/*!
 * Name:    Alex Wolski
 * Date:    September 22, 2021
 * Desc:    A chain replica node program managed by a ZooKeeper instance
 * 
 * Boilerplate code from the article: A beginner's guide to gRPC with Rust
 * https://dev.to/anshulgoyal15/a-beginners-guide-to-grpc-with-rust-3c7o
 * https://github.com/anshulrgoyal/rust-grpc-demo
!*/


//Import the rust file generated from the proto
pub mod chain {
    tonic::include_proto!("chain");
}

//Libraries
use std::env;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::{task::JoinHandle};
use tonic::{transport::Server, Request, Response, Status};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper, ZkState};

//Head
use chain::head_chain_replica_server::{HeadChainReplica, HeadChainReplicaServer};
use chain::head_chain_replica_client::HeadChainReplicaClient;
use chain::{IncRequest, HeadResponse};
//Tail
use chain::tail_chain_replica_server::{TailChainReplica, TailChainReplicaServer};
use chain::tail_chain_replica_client::TailChainReplicaClient;
use chain::{GetRequest, GetResponse};
//Replica
use chain::replica_server::{Replica, ReplicaServer};
use chain::replica_client::ReplicaClient;
use chain::{UpdateRequest, UpdateResponse};
use chain::{StateTransferRequest, StateTransferResponse};
use chain::{AckRequest, AckResponse};

//The port all services will run on
static SOCKET_ADDRESS: &str = "[::1]:50051";
//Name of the author
static NAME: &str = "Alex Wolski";
//The delimiting character separating the address and name in the znode data
static ZNODE_DELIM: &str = "\n";
//The znode name prefix for all replicas
static ZNODE_PREFIX: &str = "replica-";
//The length of the sequence number ZooKeeper adds to znodes
static SEQUENCE_LEN: u32 = 10;


#[tonic::async_trait]
pub trait GRpcServer {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    fn is_running(&self) -> bool;
}

pub struct HeadChainReplicaService {
    data: Arc<RwLock<HashMap<String, i32>>>
}

#[tonic::async_trait]
impl HeadChainReplica for HeadChainReplicaService {
    async fn increment(&self, request: Request<IncRequest>) ->
    Result<Response<HeadResponse>, Status> {
        println!("Received Inc Request. Key: {}, Value: {}", request.get_ref().key, request.get_ref().inc_value);

        let head_response = chain::HeadResponse {
            rc: 0
        };

        Ok(Response::new(head_response))
    }
}

pub struct HeadServerManager {
    data: Arc<RwLock<HashMap<String, i32>>>,
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl HeadServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<HeadServerManager, Box<dyn std::error::Error>> {
        Ok(HeadServerManager {
            data: data,
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for HeadServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let head_service = HeadChainReplicaService { data: self.data.clone() };
        let head_server = Server::builder().add_service(HeadChainReplicaServer::new(head_service));

        let _ = self.join_handle.insert(tokio::spawn(async move {
            let _ = head_server.serve(socket).await;
        }));

        self.running = true;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.join_handle {
            Some(handle) => {
                handle.abort();
                self.running = false;
                Ok(())
            }

            None => return Err(Error::new(ErrorKind::Other, format!("Cannot stop a server that isn't running.")).into()),
        }
    }

    fn is_running(&self) -> bool {
        return self.running;
    }
}


pub struct TailChainReplicaService {
    data: Arc<RwLock<HashMap<String, i32>>>
}

#[tonic::async_trait]
impl TailChainReplica for TailChainReplicaService {
    async fn get(&self, request: Request<GetRequest>) ->
    Result<Response<GetResponse>, Status> {
        println!("Received Get Request. Key: {}", request.get_ref().key);

        let tail_response = chain::GetResponse {
            rc: 0,
            value: 0
        };

        Ok(Response::new(tail_response))
    }
}


pub struct TailServerManager {
    data: Arc<RwLock<HashMap<String, i32>>>,
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl TailServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<TailServerManager, Box<dyn std::error::Error>> {
        Ok(TailServerManager {
            data: data,
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for TailServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let tail_service = TailChainReplicaService { data: self.data.clone() };
        let tail_server = Server::builder().add_service(TailChainReplicaServer::new(tail_service));

        let _ = self.join_handle.insert(tokio::spawn(async move {
            let _ = tail_server.serve(socket).await;
        }));

        self.running = true;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.join_handle {
            Some(handle) => {
                handle.abort();
                self.running = false;
                Ok(())
            }

            None => return Err(Error::new(ErrorKind::Other, format!("Cannot stop a server that isn't running.")).into()),
        }
    }

    fn is_running(&self) -> bool {
        return self.running;
    }
}


pub struct ReplicaService {
    data: Arc<RwLock<HashMap<String, i32>>>
}

#[tonic::async_trait]
impl Replica for ReplicaService {
    async fn update(&self, request: Request<UpdateRequest>) ->
    Result<Response<UpdateResponse>, Status> {
        println!("Received Update Request. Key: {}, newValue: {}, xID: {}", request.get_ref().key, request.get_ref().new_value, request.get_ref().xid);

        let update_response = chain::UpdateResponse {
            rc: 0
        };

        Ok(Response::new(update_response))
    }

    async fn state_transfer(&self, request: Request<StateTransferRequest>) ->
    Result<Response<StateTransferResponse>, Status> {
        println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

        let transfer_response = chain::StateTransferResponse {
            rc: 0
        };

        Ok(Response::new(transfer_response))
    }

    async fn ack(&self, request: Request<AckRequest>) ->
    Result<Response<AckResponse>, Status> {
        println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

        let ack_response = chain::AckResponse {};

        Ok(Response::new(ack_response))
    }
}

pub struct ReplicaServerManager {
    data: Arc<RwLock<HashMap<String, i32>>>,
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl ReplicaServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<ReplicaServerManager, Box<dyn std::error::Error>> {
        Ok(ReplicaServerManager {
            data: data,
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for ReplicaServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let replica_service = ReplicaService { data: self.data.clone() };
        let replica_server = Server::builder().add_service(ReplicaServer::new(replica_service));

        let _ = self.join_handle.insert(tokio::spawn(async move {
            let _ = replica_server.serve(socket).await;
        }));

        self.running = true;

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.join_handle {
            Some(handle) => {
                handle.abort();
                self.running = false;
                Ok(())
            }

            None => return Err(Error::new(ErrorKind::Other, format!("Cannot stop a server that isn't running.")).into()),
        }
    }

    fn is_running(&self) -> bool {
        self.running
    }
}


mod zk_manager {
    use super::*;

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
            zk_manager::NoopWatcher);

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
}

mod replica_manager {
    use super::*;

    pub struct Replica {
        zk_instance: ZooKeeper,
        socket: SocketAddr,
        base_path: String,
        replica_id: u32,
        replica_data: Arc<RwLock<HashMap<String, i32>>>,
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

        //Returns the sequence number of a replica znode
        fn get_replica_id(znode: &str) -> Result<u32, Box<dyn std::error::Error>> {
            let mut znode_str = znode.to_string();
            //Find the starting posiiton of the znode sequence
            let split_point = znode_str.len() - (SEQUENCE_LEN as usize);
            let id_string = znode_str.split_off(split_point);
            let replica_id = id_string.parse::<u32>();

            match replica_id {
                Ok(id) => Ok(id),
                _ => return Err(Error::new(ErrorKind::Other,
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
                        return Err(Error::new(ErrorKind::Other,
                            format!("znode data is missing an address")).into())
                    }
                },
                None => return Err(Error::new(ErrorKind::Other,
                    format!("znode data '{}' is formatted incorrectly", znode_data)).into())
            }

            let delim_pos = result.unwrap() - 1;
            //Split the address into znode_data_str 
            let name = znode_data_str.split_off(delim_pos);

            Ok(znode_data_str)
        }

        // Takes a znode path and returns the address
        fn get_node_address(&self, znode: String) -> Result<String, Box<dyn std::error::Error>> {
                let result = self.zk_instance.get_data(&znode, false);

                match result {
                    Ok(node_data) => {
                        let data_str = String::from_utf8(node_data.0)?;
                        let address = Replica::parse_znode_addr(&data_str)?;

                        Ok(address)
                    }
                    _ => {
                        Err(Error::new(ErrorKind::Other,
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
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children: {}", &self.base_path)).into())
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
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children: {}", &self.base_path)).into())
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
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children: {}", &self.base_path)).into())
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
            Err(Error::new(ErrorKind::Other, format!("Invalid chain position: {}", replica_index)).into())
        }


        fn get_succ(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
            //Get all children of the base node
            let result = self.zk_instance.get_children(&self.base_path, false);

            //Handle a connection error
            match result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::Other, format!("Failed to get children: {}", &self.base_path)).into())
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

        pub fn new(host_list: &str, base_path: &str, socket: SocketAddr)
        -> Result<Replica, Box<dyn std::error::Error>> {
            //Construct the replica znode path (before the sequence number is added)
            let znode_path = format!("{}/{}", base_path, ZNODE_PREFIX);
            //Construct the contents of the znode
            let znode_data = format!("{}{}{}", SOCKET_ADDRESS, ZNODE_DELIM, NAME);

            //Connect to the ZooKeeper host
            let mut instance = zk_manager::connect(host_list, Replica::print_conn_state, 5)?;
            //Create a new zNode for this replica
            let znode = zk_manager::create(&mut instance, &znode_path, &znode_data, CreateMode::EphemeralSequential)?;

            //Create the shared data for the servers
            let replica_data = Arc::new(RwLock::new(HashMap::<String, i32>::new()));

            //Create servers for the head, tail, and replica service
            let head_server = HeadServerManager::new(replica_data.clone())?;
            let tail_server = TailServerManager::new(replica_data.clone())?;
            let replica_server = ReplicaServerManager::new(replica_data.clone())?;

            Ok(Replica{
                zk_instance: instance,
                socket: socket,
                base_path: base_path.to_string(),
                replica_id: Replica::get_replica_id(&znode)?,
                replica_data: replica_data.clone(),
                head_server: head_server,
                tail_server: tail_server,
                replica_server: replica_server,
            })
        }

        pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            //The replica service should always be running
            println!("Starting replica server");
            self.replica_server.start(self.socket.clone())?;

            //Run the head and tail servers based on the position of the replica in the chain
            if self.is_head()? {
                println!("Starting head server");
                self.head_server.start(self.socket.clone())?;
            }
            if self.is_tail()? {
                println!("Starting tail server");
                self.tail_server.start(self.socket.clone())?;
            }

            let pred = self.get_pred()?;

            match pred {
                Some(pred_znode) => {
                    let full_path = format!("{}/{}", self.base_path, pred_znode);
                    let address = self.get_node_address(full_path)?;
                    println!("znode address: {}", address);
                    Ok(())
                }
                _ => {
                    println!("No pred");
                    Ok(())
                }
            }

        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket: SocketAddr = SOCKET_ADDRESS.parse()?;
    let args: Vec<String> = env::args().collect();

    if args.len() != 3
    {
        println!("Correct Usage: chain_replica.rs ZOOKEEPER_HOST_PORT_LIST base_path");
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid number of arguments").into());
    }

    let mut replica = replica_manager::Replica::new(&args[1], &args[2], socket)?;
    replica.start()?;

    loop{}

    Ok(())
}