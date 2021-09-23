//Import the rust file generated from the proto
pub mod chain {
    tonic::include_proto!("chain");
}

//Libraries
use std::io::{Error, ErrorKind};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use tokio::{task::JoinHandle};
use tonic::{transport::Server, Request, Response, Status};

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

#[tonic::async_trait]
pub trait GRpcServer {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>>;
    fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    fn is_running(&self) -> bool;
}

pub struct replica_data {
    pub database: Arc<RwLock<HashMap<String, i32>>>,
    pub sent: Arc<RwLock<Vec<(String, i32, u32)>>>,
    pub ack: Arc<RwLock<Vec<u32>>>,
    pub pred_addr: Arc<RwLock<Option<String>>>,
    pub succ_addr: Arc<RwLock<Option<String>>>,
    pub new_tail: Arc<RwLock<bool>>,
}

pub struct HeadChainReplicaService {
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
}

#[tonic::async_trait]
impl HeadChainReplica for HeadChainReplicaService {
    async fn increment(&self, request: Request<IncRequest>) ->
    Result<Response<HeadResponse>, Status> {
        let active_read = self.active.read().unwrap();

        if *active_read {
            println!("Received Inc Request. Key: {}, Value: {}", request.get_ref().key, request.get_ref().inc_value);
            let head_response = chain::HeadResponse { rc: 0 };
            Ok(Response::new(head_response))
        }
        else {
            let head_response = chain::HeadResponse { rc: 1 };
            Ok(Response::new(head_response))
        }
    }
}

pub struct HeadServerManager {
    //Service data
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
    //Server data
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl HeadServerManager {
    pub fn new(shared_data: Arc<replica_data>, active: Arc<RwLock<bool>>)
    -> Result<HeadServerManager, Box<dyn std::error::Error>> {
        Ok(HeadServerManager {
            shared_data: shared_data.clone(),
            active: active.clone(),
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for HeadServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let head_service = HeadChainReplicaService {
            shared_data: self.shared_data.clone(),
            active: self.active.clone(),
        };

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
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
}

#[tonic::async_trait]
impl TailChainReplica for TailChainReplicaService {
    async fn get(&self, request: Request<GetRequest>) ->
    Result<Response<GetResponse>, Status> {
        let active_read =  self.active.read().unwrap();

        if *active_read {
            println!("Received Get Request. Key: {}", request.get_ref().key);

            let tail_response = chain::GetResponse {
                rc: 0,
                value: 0,
            };

            Ok(Response::new(tail_response))
        }
        else {
            let tail_response = chain::GetResponse { rc: 1, value : 0 };
            Ok(Response::new(tail_response))
        }
    }
}


pub struct TailServerManager {
    //Service data
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
    //Server data
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl TailServerManager {
    pub fn new(shared_data: Arc<replica_data>, active: Arc<RwLock<bool>>)
    -> Result<TailServerManager, Box<dyn std::error::Error>> {
        Ok(TailServerManager {
            shared_data: shared_data.clone(),
            active: active.clone(),
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for TailServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let tail_service = TailChainReplicaService {
            shared_data: self.shared_data.clone(),
            active: self.active.clone(),
        };

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
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
}

#[tonic::async_trait]
impl Replica for ReplicaService {
    async fn update(&self, request: Request<UpdateRequest>) ->
    Result<Response<UpdateResponse>, Status> {
        let new_tail_read = self.shared_data.new_tail.read().unwrap();

        //If the replica is a new tail, notify the predecesor to send a state transfer
        if *new_tail_read {
            let update_response = chain::UpdateResponse { rc: 1 };
            Ok(Response::new(update_response))
        }
        //Otherwise, forward the update request
        else {
            println!("Received Update Request. Key: {}, newValue: {}, xID: {}",
                request.get_ref().key, request.get_ref().new_value, request.get_ref().xid);
            let update_response = chain::UpdateResponse { rc: 0 };
            Ok(Response::new(update_response))
        }
    }

    async fn state_transfer(&self, request: Request<StateTransferRequest>) ->
    Result<Response<StateTransferResponse>, Status> {
        let new_tail_read = self.shared_data.new_tail.read().unwrap();

        //If this replica is a new node, accept the state transfer
        if *new_tail_read {
            println!("Received State Transfer Request. xID: {}", request.get_ref().xid);
            let transfer_response = chain::StateTransferResponse { rc: 0 };

            //Release the read lock
            drop(new_tail_read);
            //Aquire a write lock and set new_tail to false
            match self.shared_data.new_tail.write() {
                Ok(mut new_tail_write) => *new_tail_write = false,
                _ => panic!("Failed to write 'new_tail'")
            }

            Ok(Response::new(transfer_response))
        }
        //Otherwise, notify the predecessor that this replica isn't a new tail
        else {
            let transfer_response = chain::StateTransferResponse { rc: 1 };
            Ok(Response::new(transfer_response))
        }
    }

    async fn ack(&self, request: Request<AckRequest>) ->
    Result<Response<AckResponse>, Status> {
        println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

        let ack_response = chain::AckResponse {};

        Ok(Response::new(ack_response))
    }
}

pub struct ReplicaServerManager {
    //Service data
    shared_data: Arc<replica_data>,
    active: Arc<RwLock<bool>>,
    //Server data
    join_handle: Option<JoinHandle<()>>,
    running: bool,
}

impl ReplicaServerManager {
    pub fn new(shared_data: Arc<replica_data>, active: Arc<RwLock<bool>>)
    -> Result<ReplicaServerManager, Box<dyn std::error::Error>> {
        Ok(ReplicaServerManager {
            shared_data: shared_data.clone(),
            active: active.clone(),
            join_handle: None,
            running: false,
        })
    }
}

#[tonic::async_trait]
impl GRpcServer for ReplicaServerManager {
    fn start(&mut self, socket: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let replica_service = ReplicaService {
            shared_data: self.shared_data.clone(),
            active: self.active.clone(),
        };
        
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