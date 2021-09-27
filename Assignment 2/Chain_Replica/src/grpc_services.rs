//Import the rust file generated from the proto
pub mod chain {
    tonic::include_proto!("chain");
}

//Libraries
use std::collections::HashMap;
use std::net::SocketAddr;
use async_std::sync::{Arc, RwLock};
use tonic::{transport::Server, Request, Response, Status, Code};
use tokio::time::{sleep, Duration};
use triggered::{Trigger, Listener};

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

//The number of milliseconds to wait before retrying a connection
static RETRY_WAIT: u64 = 1000;

pub struct ReplicaData {
    pub database: Arc<RwLock<HashMap<String, i32>>>,
    pub xid: Arc<RwLock<u32>>,
    pub sent: Arc<RwLock<Vec<Request<UpdateRequest>>>>,
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
        let is_paused_read = self.is_paused.read().await;

        //Ignore the request if the replica is not the head
        if *is_paused_read {
            #[cfg(debug_assertions)]
            println!("Received IncRequest, but this replica is not the head");

            let head_response = chain::HeadResponse { rc: 1 };
            Ok(Response::new(head_response))
        }
        else {
            let reqeust_ref = request.get_ref();

            #[cfg(debug_assertions)]
            println!("Received IncRequest. Key: {}, Value: {}", reqeust_ref.key, reqeust_ref.inc_value);

            //Get the logical clock
            let xid_read = self.shared_data.xid.read().await;
            let curr_xid = *xid_read;
            drop(xid_read);

            //Get the current value
            let data_read = self.shared_data.database.read().await;

            let old_value = match data_read.get(&reqeust_ref.key) {
                Some(value) => *value,
                None => 0,
            };

            drop(data_read);

            //Compute the new value
            let new_value = old_value + reqeust_ref.inc_value;

            //Construct the update request
            let update_request = Request::new(UpdateRequest {
                key: reqeust_ref.key.clone(),
                new_value: new_value,
                //Send the incremented logical clock, but don't modify it yet
                xid: curr_xid + 1,
            });

            //Connect to the local replica service
            let uri = format!("https://{}", self.shared_data.my_addr);
            let connect_result = ReplicaClient::connect(uri).await;

            let mut replica_client = match connect_result {
                Ok(client) => client,
                //Ignore the IncRequest if a connection could not be established
                Err(_) => {
                    #[cfg(debug_assertions)]
                    println!("Failed to connect to the local replica service. Aborting operation.");
                    return Err(Status::new(Code::Aborted, "Replica service not responding"))
                },
            };

            //Send the UpdateRequest
            let update_response = replica_client.update(update_request).await;

            match update_response {
                Ok(_) => {},
                //Ignore the IncRequest if UpdateRequest fails
                Err(_) => {
                    #[cfg(debug_assertions)]
                    println!("Failed to send an UpdateRequest to the local replica service. Aborting operation.");
                    return Err(Status::new(Code::Aborted, "UpdateRequest failed"))
                },
            }

            //Return an IncResponse
            let head_response = chain::HeadResponse { rc: 0 };
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
        let is_paused_read =  self.is_paused.read().await;

        if *is_paused_read {
            #[cfg(debug_assertions)]
            println!("Received GetRequest, but this replica is not the tail");

            let tail_response = chain::GetResponse { rc: 1, value : 0 };
            Ok(Response::new(tail_response))
        }
        else {
            let key = &request.get_ref().key;

            #[cfg(debug_assertions)]
            println!("Received Get Request. Key: {}", key);

            let data_read = self.shared_data.database.read().await;

            let value = match data_read.get(key) {
                Some(value) => value,
                None => &0,
            };

            let tail_response = chain::GetResponse {
                rc: 0,
                value: *value,
            };

            Ok(Response::new(tail_response))
        }
    }
}


pub struct ReplicaService {
    shared_data: Arc<ReplicaData>,
    is_paused: Arc<RwLock<bool>>,
}

impl ReplicaService {
    //Forward the update request to the successor server
    async fn forward_update(shared_data: Arc<ReplicaData>, request: Request<UpdateRequest>) {
        let request_ref = request.get_ref();

        loop {
            //Read the ip address of the successor
            let succ_addr_read = shared_data.succ_addr.read().await;

            let succ_addr = match &(*succ_addr_read) {
                Some(addr) => addr.clone(),
                //Discard the update if there is no successor
                None => return,
            };

            drop(succ_addr_read);

            //Connect to the successor replica service
            let uri = format!("https://{}", succ_addr);
            let connect_result = ReplicaClient::connect(uri).await;

            match connect_result {
                Ok(mut succ_client) => {
                    //Copy the update requset
                    let request_clone = Request::new(UpdateRequest {
                        key: request_ref.key.clone(),
                        new_value: request_ref.new_value.clone(),
                        xid: request_ref.xid.clone(),
                    });

                    //Attempt to send the UpdateRequest
                    let update_result = succ_client.update(request_clone).await;

                    match update_result {
                        Ok(response) => {
                            match response.get_ref().rc {
                                //Update successfully reached the successor
                                0 => return,
                                //State transfer requested. The successor has changed.
                                1 => {
                                    //To-do: Send a state transfer
                                }
                                //Invalid status code
                                rc => {
                                    #[cfg(debug_assertions)]
                                    println!("Successor returned UpdateResponse with invalid code: '{}'. Retrying...", rc);
                                    sleep(Duration::from_millis(RETRY_WAIT)).await;
                                }
                            }
                        },
                        //If the request could not be sent, retry
                        Err(_) => {
                            #[cfg(debug_assertions)]
                            println!("Failed to send an UpdateRequest to the successor at address: '{}'. Retrying...", succ_addr);
                            sleep(Duration::from_millis(RETRY_WAIT)).await;
                        },
                    }
                },
                //If a connection could not be established, retry
                Err(_) => {
                    #[cfg(debug_assertions)]
                    println!("Failed to connect to the successor at address: '{}'. Retrying...", succ_addr);
                    sleep(Duration::from_millis(RETRY_WAIT)).await;
                },
            };
        }
    }
}

#[tonic::async_trait]
impl Replica for ReplicaService {
    async fn update(&self, request: Request<UpdateRequest>) ->
    Result<Response<UpdateResponse>, Status> {
        let new_tail_read = self.shared_data.new_tail.read().await;

        if *new_tail_read {
            #[cfg(debug_assertions)]
            println!("Received Update, requesting state transfer");

            let update_response = chain::UpdateResponse { rc: 1 };
            Ok(Response::new(update_response))
        }
        else {
            let request_ref = request.get_ref();
            #[cfg(debug_assertions)]
            println!("Received Update Request. Key: {}, newValue: {}, xID: {}",
                request_ref.key, request_ref.new_value, request_ref.xid);

            //Get the logical clock
            let xid_read = self.shared_data.xid.read().await;
            let curr_xid = *xid_read;
            drop(xid_read);

            //Check if the update xid is outdated
            if request_ref.xid <= curr_xid {
                #[cfg(debug_assertions)]
                println!("The update xID '{}' is older than the current xID '{}'. Aborting operation.", request_ref.xid, curr_xid);
                return Err(Status::new(Code::InvalidArgument, "Old xID"));
            }

            //If the update xid is too new, request a state transfer
            if request_ref.xid > curr_xid + 1 {
                let update_response = chain::UpdateResponse { rc: 1 };
                return Ok(Response::new(update_response))
            }

            //Update the value
            let mut data_write = self.shared_data.database.write().await;
            let _old_value = (*data_write).insert(request_ref.key.clone(), request_ref.new_value);
            drop(data_write);

            //Update the xid
            let mut xid_write = self.shared_data.xid.write().await;
            *xid_write = request_ref.xid;
            drop(xid_write);

            //Copy the update requset
            let request_clone = Request::new(UpdateRequest {
                key: request_ref.key.clone(),
                new_value: request_ref.new_value.clone(),
                xid: request_ref.xid.clone(),
            });

            //Add the update to the sent list
            let mut sent_write = self.shared_data.sent.write().await;
            (*sent_write).push(request_clone);
            drop(sent_write);

            //Forward the update
            let shared_data_clone = self.shared_data.clone();

            tokio::spawn(async move {
                ReplicaService::forward_update(shared_data_clone, request).await
            });

            //Return an UpdateResponse
            let update_response = chain::UpdateResponse { rc: 0 };
            Ok(Response::new(update_response))
        }
    }

    async fn state_transfer(&self, request: Request<StateTransferRequest>) ->
    Result<Response<StateTransferResponse>, Status> {
        let new_tail_read = self.shared_data.new_tail.read().await;

        //If this replica is a new node, accept the state transfer
        if *new_tail_read {
            #[cfg(debug_assertions)]
            println!("Received State Transfer Request. xID: {}", request.get_ref().xid);

            let transfer_response = chain::StateTransferResponse { rc: 0 };

            //Release the read lock
            drop(new_tail_read);
            //Aquire a write lock and set new_tail to false
            let mut new_tail_write = self.shared_data.new_tail.write().await;
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
    
    pub async fn start(&mut self, socket: SocketAddr, head_paused: bool, tail_paused: bool, replica_paused: bool) ->
    Result<(), Box<dyn std::error::Error>> {
        //Set the paused status of all three services
        if head_paused {
            self.pause(ChainService::HEAD).await?;
        }
        if tail_paused {
            self.pause(ChainService::TAIL).await?;
        }
        if replica_paused {
            self.pause(ChainService::REPLICA).await?;
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

    pub async fn pause(&mut self, service: ChainService) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Pausing head service");

        let mut paused_write = match service {
            ChainService::HEAD => self.head_paused.write().await,
            ChainService::TAIL => self.tail_paused.write().await,
            ChainService::REPLICA => self.replica_paused.write().await,
        };

        *paused_write = true;
        Ok(())
    }

    pub async fn resume(&mut self, service: ChainService) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Resuming head service");

        let mut paused_write = match service {
            ChainService::HEAD => self.head_paused.write().await,
            ChainService::TAIL => self.tail_paused.write().await,
            ChainService::REPLICA => self.replica_paused.write().await,
        };
        
        *paused_write = false;
        Ok(())
    }
}