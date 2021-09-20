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
use tonic::{transport::Server, Request, Response, Status};
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};

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

pub struct ZooKeeperClient {
    zk_instance: zookeeper::ZooKeeper,
}

impl ZooKeeperClient {
    fn conn_listener(state: zookeeper::ZkState) {
        println!("New ZkState is {:?}", state);
    }

    fn connect(host_list: &str, timeout: u64)
    -> Result<ZooKeeperClient, Box<dyn std::error::Error>> {
        struct LoggingWatcher;
        impl Watcher for LoggingWatcher {
            fn handle(&self, e: WatchedEvent) {
                println!("Disconnected from ZooKeeper: {:?}", e)
            }
        }

        //Connecting to ZooKeeper
        let connect_result = ZooKeeper::connect(
            host_list,
            std::time::Duration::from_secs(timeout),
            LoggingWatcher);

        //Handle a connection error
        match connect_result {
            Ok(_) => println!("Connecting to master: '{}'...", host_list),
            Err(_) => return Err(Error::new(ErrorKind::InvalidInput,
                format!("The host list '{}' is not valid.", host_list)).into())
        };

        let instance = connect_result.unwrap();
        instance.add_listener(ZooKeeperClient::conn_listener);

        Ok(ZooKeeperClient {
            zk_instance: instance,
        })
    }

    fn create(&self, path: &str, znode_data: &str, create_mode: CreateMode)
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


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket : std::net::SocketAddr = SOCKET_ADDRESS.parse()?;
    let args: Vec<String> = env::args().collect();

    //Shared hashmap protected by a mutex
    let data = Arc::new(RwLock::new(HashMap::<String, i32>::new()));

    if args.len() != 3
    {
        println!("Correct Usage: chain_replica.rs ZOOKEEPER_HOST_PORT_LIST CONTROL_PATH");
        return Err(Error::new(ErrorKind::InvalidInput, "Invalid number of arguments").into());
    }

    let znode_data = format!("{}\n{}", SOCKET_ADDRESS, NAME);
    let zk_client = ZooKeeperClient::connect(&args[1], 5)?;
    zk_client.create(&args[2], &znode_data, CreateMode::EphemeralSequential)?;

    //Instantiate services with the shared data
    let replica_service = ReplicaService { data: data.clone() };
    let head_service = HeadChainReplicaService { data: data.clone() };
    let tail_service = TailChainReplicaService { data: data.clone() };

    //Create servers to run the services
    let replica_server = Server::builder().add_service(ReplicaServer::new(replica_service));
    let head_server = Server::builder().add_service(HeadChainReplicaServer::new(head_service));
    let tail_server = Server::builder().add_service(TailChainReplicaServer::new(tail_service));

    println!("Creating replica service");
    replica_server.serve(socket).await?;

    Ok(())
}