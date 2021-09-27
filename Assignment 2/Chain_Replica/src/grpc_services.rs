//Import the rust file generated from the proto
pub mod chain {
    tonic::include_proto!("chain");
}

//Libraries
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};
use triggered::{Trigger, Listener};

//Head
use chain::head_chain_replica_server::{HeadChainReplica, HeadChainReplicaServer};
use chain::{IncRequest, HeadResponse};
//Tail
use chain::tail_chain_replica_server::{TailChainReplica, TailChainReplicaServer};
use chain::{GetRequest, GetResponse};
//Replica
use chain::replica_server::{Replica, ReplicaServer};
use chain::{UpdateRequest, UpdateResponse};
use chain::{StateTransferRequest, StateTransferResponse};
use chain::{AckRequest, AckResponse};

pub struct ReplicaData {
    pub database: Arc<RwLock<HashMap<String, i32>>>,
    pub sent: Arc<RwLock<Vec<(String, i32, u32)>>>,
    pub ack: Arc<RwLock<Vec<u32>>>,
    pub my_addr: String,
    pub pred_addr: Arc<RwLock<Option<String>>>,
    pub succ_addr: Arc<RwLock<Option<String>>>,
    pub new_tail: Arc<RwLock<bool>>,
}

pub struct HeadChainReplicaService {
    shared_data: Arc<ReplicaData>,
    is_paused: Arc<RwLock<bool>>,
}

#[tonic::async_trait]
impl HeadChainReplica for HeadChainReplicaService {
    async fn increment(&self, request: Request<IncRequest>) ->
    Result<Response<HeadResponse>, Status> {
        let is_paused_read = self.is_paused.read().unwrap();

        if *is_paused_read {
            #[cfg(debug_assertions)]
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


pub struct TailChainReplicaService {
    shared_data: Arc<ReplicaData>,
    is_paused: Arc<RwLock<bool>>,
}

#[tonic::async_trait]
impl TailChainReplica for TailChainReplicaService {
    async fn get(&self, request: Request<GetRequest>) ->
    Result<Response<GetResponse>, Status> {
        let is_paused_read =  self.is_paused.read().unwrap();

        if *is_paused_read {
            #[cfg(debug_assertions)]
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


pub struct ReplicaService {
    shared_data: Arc<ReplicaData>,
    is_paused: Arc<RwLock<bool>>,
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
            #[cfg(debug_assertions)]
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
            #[cfg(debug_assertions)]
            println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

            let transfer_response = chain::StateTransferResponse { rc: 0 };

            //Release the read lock
            drop(new_tail_read);
            //Aquire a write lock and set new_tail to false
            let mut new_tail_write = self.shared_data.new_tail.write().unwrap();
            *new_tail_write = false;

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
        #[cfg(debug_assertions)]
        println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

        let ack_response = chain::AckResponse {};

        Ok(Response::new(ack_response))
    }
}


pub enum ChainService {
    HEAD,
    TAIL,
    REPLICA
}

pub struct ServerManager {
    //Service data
    shared_data: Arc<ReplicaData>,
    head_paused: Arc<RwLock<bool>>,
    tail_paused: Arc<RwLock<bool>>,
    replica_paused: Arc<RwLock<bool>>,
    //Server data
    shutdown_trigger: Trigger,
    shutdown_listener: Listener,
}

impl ServerManager {
    pub fn new(shared_data: Arc<ReplicaData>)
    -> Result<ServerManager, Box<dyn std::error::Error>> {
        let (trigger, listener) = triggered::trigger();

        Ok(ServerManager {
            shared_data: shared_data.clone(),
            head_paused: Arc::new(RwLock::new(false)),
            tail_paused: Arc::new(RwLock::new(false)),
            replica_paused: Arc::new(RwLock::new(false)),
            shutdown_trigger: trigger,
            shutdown_listener: listener,
        })
    }
    
    pub fn start(&mut self, socket: SocketAddr, head_paused: bool, tail_paused: bool, replica_paused: bool) ->
    Result<(), Box<dyn std::error::Error>> {
        //Set the paused status of all three services
        if head_paused {
            self.pause(ChainService::HEAD)?;
        }
        if tail_paused {
            self.pause(ChainService::TAIL)?;
        }
        if replica_paused {
            self.pause(ChainService::REPLICA)?;
        }

        //Create the service structs
        let head_service = HeadChainReplicaService {
            shared_data: self.shared_data.clone(),
            is_paused: self.head_paused.clone(),
        };

        let tail_service = TailChainReplicaService {
            shared_data: self.shared_data.clone(),
            is_paused: self.tail_paused.clone(),
        };

        let replica_service = ReplicaService {
            shared_data: self.shared_data.clone(),
            is_paused: self.replica_paused.clone(),
        };

        //Add all three services to a server
        let head_server = Server::builder()
            .add_service(HeadChainReplicaServer::new(head_service))
            .add_service(TailChainReplicaServer::new(tail_service))
            .add_service(ReplicaServer::new(replica_service));

        //Start the server
        let listener = self.shutdown_listener.clone();

        let _ = tokio::spawn(async move {
            let _ = head_server.serve_with_shutdown(socket, listener).await;
        });

        Ok(())
    }

    pub fn stop(self,) {
        self.shutdown_trigger.trigger();
    }

    pub fn pause(&mut self, service: ChainService) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Pausing head service");

        let mut paused_write = match service {
            ChainService::HEAD => self.head_paused.write().unwrap(),
            ChainService::TAIL => self.tail_paused.write().unwrap(),
            ChainService::REPLICA => self.replica_paused.write().unwrap(),
        };

        *paused_write = true;
        Ok(())
    }

    pub fn resume(&mut self, service: ChainService) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Resuming head service");

        let mut paused_write = match service {
            ChainService::HEAD => self.head_paused.write().unwrap(),
            ChainService::TAIL => self.tail_paused.write().unwrap(),
            ChainService::REPLICA => self.replica_paused.write().unwrap(),
        };
        
        *paused_write = false;
        Ok(())
    }
}