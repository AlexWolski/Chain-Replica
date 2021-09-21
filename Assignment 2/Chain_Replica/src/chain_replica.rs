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
use tonic::{transport::Server, transport::server, Request, Response, Status};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper, ZkState};

//Head
use chain::head_chain_replica_server::{HeadChainReplica, HeadChainReplicaServer};
use chain::{IncRequest, HeadResponse};
//Tail
use chain::tail_chain_replica_server::{TailChainReplica, TailChainReplicaServer};
use chain::{GetRequest, GetResponse};
//Replica Server
use chain::replica_server::{Replica, ReplicaServer};
use chain::{UpdateRequest, UpdateResponse};
use chain::{StateTransferRequest, StateTransferResponse};
use chain::{AckRequest, AckResponse};

//The port all services will run on
static SOCKET_ADDRESS : &str = "[::1]:50051";
//Name of the author
static NAME : &str = "Alex Wolski";


#[tonic::async_trait]
pub trait GRpcServer {
    async fn start(self, socket : SocketAddr) -> Result<(), Box<dyn std::error::Error>>;
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
    server: server::Router<HeadChainReplicaServer<HeadChainReplicaService>, server::Unimplemented>,
}

impl HeadServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<HeadServerManager, Box<dyn std::error::Error>> {
        let head_service = HeadChainReplicaService { data: data };
        let head_server = Server::builder().add_service(HeadChainReplicaServer::new(head_service));

        Ok(HeadServerManager {
            server: head_server,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for HeadServerManager {
    async fn start(self, socket : SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        self.server.serve(socket).await?;
        Ok(())
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
    server: server::Router<TailChainReplicaServer<TailChainReplicaService>, server::Unimplemented>,
}

impl TailServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<TailServerManager, Box<dyn std::error::Error>> {
        let tail_service = TailChainReplicaService { data: data };
        let tail_server = Server::builder().add_service(TailChainReplicaServer::new(tail_service));

        Ok(TailServerManager {
            server: tail_server,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for TailServerManager {
    async fn start(self, socket : SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        self.server.serve(socket).await?;
        Ok(())
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
    server: server::Router<ReplicaServer<ReplicaService>, server::Unimplemented>,
}

impl ReplicaServerManager {
    pub fn new(data: Arc<RwLock<HashMap<String, i32>>>)
    -> Result<ReplicaServerManager, Box<dyn std::error::Error>> {
        let replica_service = ReplicaService { data: data };
        let replica_server = Server::builder().add_service(ReplicaServer::new(replica_service));

        Ok(ReplicaServerManager {
            server: replica_server,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for ReplicaServerManager {
    async fn start(self, socket : SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        self.server.serve(socket).await?;
        Ok(())
    }
}


mod zk_manager {
    use super::*;

    //Required for creating a connection, but not used
    struct NoopWatcher;
    impl Watcher for NoopWatcher {
        fn handle(&self, _event: WatchedEvent) {}
    }

    pub struct ZkClient {
        zk_instance: zookeeper::ZooKeeper,
    }

    impl ZkClient {
        //Listens for changes in the connection
        fn conn_listener(state: ZkState) {
            match state {
                ZkState::Connected | ZkState::ConnectedReadOnly =>
                    println!("Connected to ZooKeeper host"),
                ZkState::Closed =>
                    println!("Disconnected from ZooKeeper host"),
                _ => (),
            }
        }

        pub fn connect(host_list: &str, timeout: u64)
        -> Result<ZkClient, Box<dyn std::error::Error>> {
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
            instance.add_listener(ZkClient::conn_listener);

            Ok(ZkClient {
                zk_instance: instance,
            })
        }

        pub fn create(&self, path: &str, znode_data: &str, create_mode: CreateMode)
        -> Result<(), Box<dyn std::error::Error>> {
            let create_result = self.zk_instance.create(path,
                znode_data.as_bytes().to_vec(),
                Acl::open_unsafe().clone(),
                create_mode);

            //Handle a creation error
            match create_result {
                Ok(_) => (),
                Err(_) => return Err(Error::new(ErrorKind::InvalidData,
                    format!("Unable to create the znode: {}", path)).into())
            };

            println!("Successfully created zNode: {}", create_result.unwrap());

            Ok(())
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket : SocketAddr = SOCKET_ADDRESS.parse()?;
    let args: Vec<String> = env::args().collect();

    //Shared hashmap protected by a mutex
    let data = Arc::new(RwLock::new(HashMap::<String, i32>::new()));

    if args.len() != 3
    {
        println!("Correct Usage: chain_replica.rs ZOOKEEPER_HOST_PORT_LIST CONTROL_PATH");
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid number of arguments").into());
    }

    let znode_data = format!("{}\n{}", SOCKET_ADDRESS, NAME);
    let zk_client = zk_manager::ZkClient::connect(&args[1], 5)?;
    zk_client.create(&args[2], &znode_data, CreateMode::EphemeralSequential)?;

    println!("Creating services");
    let head_server = HeadServerManager::new(data.clone())?;
    head_server.start(socket.clone());
    let tail_server = TailServerManager::new(data.clone())?;
    tail_server.start(socket.clone());
    let replica_server = ReplicaServerManager::new(data.clone())?;
    replica_server.start(socket.clone());

    loop {

    }

    Ok(())
}