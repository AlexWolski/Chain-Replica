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
use futures::executor::block_on;
use triggered::{Trigger, Listener};

//Head
use chain::head_chain_replica_server::{HeadChainReplica, HeadChainReplicaServer};
use chain::{IncRequest, HeadResponse};
//Tail
use chain::tail_chain_replica_server::{TailChainReplica, TailChainReplicaServer};
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
    pub database: Arc<RwLock<HashMap<String, u32>>>,
    pub xid: Arc<RwLock<u32>>,
    pub sent: Arc<RwLock<Vec<UpdateRequest>>>,
    pub ack: Arc<RwLock<Vec<u32>>>,
    pub my_addr: String,
    pub pred_addr: Arc<RwLock<Option<String>>>,
    pub succ_addr: Arc<RwLock<Option<String>>>,
    pub is_head: Arc<RwLock<bool>>,
    pub is_tail: Arc<RwLock<bool>>,
    pub new_tail: Arc<RwLock<bool>>,
}

impl ReplicaData {
    pub fn new(server_addr: String) -> ReplicaData  {
        ReplicaData{
            database: Arc::new(RwLock::new(HashMap::<String, u32>::new())),
            xid: Arc::new(RwLock::new(0)),
            sent: Arc::new(RwLock::new(Vec::<UpdateRequest>::new())),
            ack: Arc::new(RwLock::new(Vec::<u32>::new())),
            my_addr: server_addr,
            pred_addr: Arc::new(RwLock::new(Option::<String>::None)),
            succ_addr: Arc::new(RwLock::new(Option::<String>::None)),
            is_head: Arc::new(RwLock::new(false)),
            is_tail: Arc::new(RwLock::new(false)),
            new_tail: Arc::new(RwLock::new(true)),
        }
    }
}

pub struct HeadChainReplicaService {
    shared_data: Arc<ReplicaData>,
}

#[tonic::async_trait]
impl HeadChainReplica for HeadChainReplicaService {
    async fn increment(&self, request: Request<IncRequest>) ->
    Result<Response<HeadResponse>, Status> {
        let is_head_read = self.shared_data.is_head.read().await;
        let is_head = *is_head_read;
        drop(is_head_read);

        //Ignore the request if the replica is not the head
        if !is_head {
            #[cfg(debug_assertions)]
            println!("Received IncRequest, but this replica is not the head");

            let head_response = chain::HeadResponse { rc: 1 };
            Ok(Response::new(head_response))
        }
        else {
            let reqeust_ref = request.get_ref();

            #[cfg(debug_assertions)]
            println!("Received IncRequest ( Key: '{}', Value: {} )", reqeust_ref.key, reqeust_ref.inc_value);

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
            let new_value = ((old_value as i32) + reqeust_ref.inc_value) as i32;

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
}

#[tonic::async_trait]
impl TailChainReplica for TailChainReplicaService {
    async fn get(&self, request: Request<GetRequest>) ->
    Result<Response<GetResponse>, Status> {
        let is_tail_read = self.shared_data.is_tail.read().await;
        let is_tail = *is_tail_read;
        drop(is_tail_read);

        //Ignore the request if the replica is not the tail
        if !is_tail {
            #[cfg(debug_assertions)]
            println!("Received GetRequest, but this replica is not the tail");

            let tail_response = chain::GetResponse { rc: 1, value : 0 };
            Ok(Response::new(tail_response))
        }
        else {
            let key = &request.get_ref().key;

            #[cfg(debug_assertions)]
            println!("Received GetRequest ( Key: '{}' )", key);

            let data_read = self.shared_data.database.read().await;

            let value = match data_read.get(key) {
                Some(value) => value,
                None => &0,
            };

            let tail_response = chain::GetResponse {
                rc: 0,
                value: *value as i32,
            };

            Ok(Response::new(tail_response))
        }
    }
}


pub struct ReplicaService {
    shared_data: Arc<ReplicaData>,
}

impl ReplicaService {
    //Sent a state transfer to the successor server
    async fn transfer_state(shared_data: Arc<ReplicaData>) {
        loop {
            //Discard the update if this is the tail
            let is_tail_read = shared_data.is_tail.read().await;
            if *is_tail_read { return };
            drop(is_tail_read);

            //Read the ip address of the successor
            let succ_addr_read = shared_data.succ_addr.read().await;
            let succ_addr = (*succ_addr_read).as_ref().unwrap().clone();
            drop(succ_addr_read);

            //Connect to the successor replica service
            let uri = format!("https://{}", succ_addr);
            let connect_result = ReplicaClient::connect(uri).await;

            match connect_result {
                Ok(mut succ_client) => {
                    //Clone state data
                    let database_read = shared_data.database.read().await;
                    let state = (*database_read).clone();
                    drop(database_read);
                    let xid_read = shared_data.xid.read().await;
                    let xid = (*xid_read).clone();
                    drop(xid_read);
                    let sent_read = shared_data.sent.read().await;
                    let sent = (*sent_read).clone();
                    drop(sent_read);

                    //Construct the state transfer requset
                    let transfer_request = Request::new(StateTransferRequest {
                        state: state,
                        xid: xid,
                        sent: sent,
                    });

                    //Attempt to send the StateTransferRequest
                    let transfer_result = succ_client.state_transfer(transfer_request).await;

                    match transfer_result {
                        Ok(response) => {
                            match response.get_ref().rc {
                                //State successfully transfered or  is no longer needed
                                0 | 1 => return,
                                //Invalid status code
                                rc => {
                                    #[cfg(debug_assertions)]
                                    println!("Successor returned StateTransferResponse with invalid code: '{}'. Retrying...", rc);
                                    sleep(Duration::from_millis(RETRY_WAIT)).await;
                                }
                            }
                        },
                        //If the request could not be sent, retry
                        Err(_) => {
                            #[cfg(debug_assertions)]
                            println!("Failed to send a StateTransferResponse to the successor at address: '{}'. Retrying...", succ_addr);
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

    //Forward the update request to the successor server
    async fn forward_update(shared_data: Arc<ReplicaData>, request: Request<UpdateRequest>) {
        let request_ref = request.get_ref();

        loop {
            //Discard the update if this is the tail
            let is_tail_read = shared_data.is_tail.read().await;
            if *is_tail_read { return };
            drop(is_tail_read);

            //Read the ip address of the successor
            let succ_addr_read = shared_data.succ_addr.read().await;
            let succ_addr = (*succ_addr_read).as_ref().unwrap().clone();
            drop(succ_addr_read);

            //Connect to the successor replica service
            let uri = format!("https://{}", succ_addr);
            let connect_result = ReplicaClient::connect(uri).await;

            match connect_result {
                Ok(mut succ_client) => {
                    //Attempt to send the UpdateRequest
                    let update_result = succ_client.update(request_ref.clone()).await;

                    match update_result {
                        Ok(response) => {
                            match response.get_ref().rc {
                                //Update successfully reached the successor
                                0 => return,
                                //State transfer requested. The successor has changed.
                                1 => {
                                    //Sent the state transfer and stop trying to send the update
                                    tokio::spawn(async move {
                                        ReplicaService::transfer_state(shared_data).await
                                    });

                                    return;
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

    async fn send_ack(shared_data: Arc<ReplicaData>, request: Request<AckRequest>) {
        let request_ref = request.get_ref();

        loop {
            //Discard the update if this is the head
            let is_head_read = shared_data.is_head.read().await;
            if *is_head_read { return };
            drop(is_head_read);

            //Read the ip address of the successor
            let pred_addr_read = shared_data.pred_addr.read().await;
            let pred_addr = (*pred_addr_read).as_ref().unwrap().clone();
            drop(pred_addr_read);

            //Connect to the predecessor replica service
            let uri = format!("https://{}", pred_addr);
            let connect_result = ReplicaClient::connect(uri).await;

            match connect_result {
                Ok(mut pred_client) => {
                    //Attempt to send the AckRequest
                    let update_result = pred_client.ack(request_ref.clone()).await;

                    match update_result {
                        //Ack was successful
                        Ok(_) => return,
                        //If the AckRequest failed, retry
                        Err(_) => {
                            #[cfg(debug_assertions)]
                            println!("Failed to send an AckRequest to the predecessor at address: '{}'. Retrying...", pred_addr);
                            sleep(Duration::from_millis(RETRY_WAIT)).await;
                        },
                    }
                },
                //If a connection could not be established, retry
                Err(_) => {
                    #[cfg(debug_assertions)]
                    println!("Failed to connect to the predecessor at address: '{}'. Retrying...", pred_addr);
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
            println!("Received UpdateRequest ( Key: '{}', newValue: {}, xID: {} )",
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
            let _ = (*data_write).insert(request_ref.key.clone(), request_ref.new_value as u32);
            drop(data_write);

            //Update the xid
            let mut xid_write = self.shared_data.xid.write().await;
            *xid_write = request_ref.xid;
            drop(xid_write);

            //Add the update to the sent list
            let mut sent_write = self.shared_data.sent.write().await;
            (*sent_write).push(request_ref.clone());
            drop(sent_write);

            //Check if this replica is the tail
            let is_tail_read = self.shared_data.is_tail.read().await;
            let is_tail = *is_tail_read;
            drop(is_tail_read);

            let shared_data_clone = self.shared_data.clone();

            //If this replica is the tail, send an ack to the predecessor
            if is_tail {
                let ack_request = Request::<chain::AckRequest>::new(chain::AckRequest { xid: curr_xid });
                ReplicaService::send_ack(shared_data_clone, ack_request).await
            }
            //If there is a successor, forward the update
            else {
                    tokio::spawn(async move {
                        ReplicaService::forward_update(shared_data_clone, request).await
                    });
            }

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
            println!("Received StateTransferRequest ( xID: {} )", request.get_ref().xid);

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
        let xid = request.get_ref().xid;
        println!("Received AckRequest ( xID: {} )", xid);

        let sent_read = self.shared_data.sent.read().await;
        let sent: &Vec<UpdateRequest> = (*sent_read).as_ref();
        let len = sent.len();
        let mut index = 0;

        //Find the update correspoinding to the xid
        for update in sent {
            if update.xid == xid {
                break;
            }

            index += 1;
        }

        drop(sent_read);

        //If the update was in the sent list, remove it
        if index < len {
            let mut sent_write = self.shared_data.sent.write().await;
            (*sent_write).remove(index);
            drop(sent_write);
        }

        Ok(Response::new(chain::AckResponse {}))
    }
}


pub struct ServerManager {
    //Service data
    shared_data: Arc<ReplicaData>,
    //Server data
    shutdown_trigger: Trigger,
    shutdown_listener: Listener,
}

impl ServerManager {
    pub fn new(shared_data: Arc<ReplicaData>) -> ServerManager  {
        let (trigger, listener) = triggered::trigger();

        ServerManager {
            shared_data: shared_data.clone(),
            shutdown_trigger: trigger,
            shutdown_listener: listener,
        }
    }
    
    pub fn start(&mut self, socket: SocketAddr, is_head: bool, is_tail: bool, new_tail: bool) {
        self.set_head(is_head);
        self.set_tail(is_tail);
        self.set_new_tail(new_tail);

        //Create the service structs
        let head_service = HeadChainReplicaService { shared_data: self.shared_data.clone() };
        let tail_service = TailChainReplicaService { shared_data: self.shared_data.clone() };
        let replica_service = ReplicaService { shared_data: self.shared_data.clone() };

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
    }

    pub fn stop(self,) {
        self.shutdown_trigger.trigger();
    }

    pub fn set_head(&mut self, is_head: bool) {
        #[cfg(debug_assertions)]
        println!("Setting head service status to: {}", is_head);

        let mut is_head_write = block_on(self.shared_data.is_head.write());
        *is_head_write = is_head;
        drop(is_head_write);
    }

    pub fn set_tail(&mut self, is_tail: bool) {
        #[cfg(debug_assertions)]
        println!("Setting tail service status to: {}", is_tail);

        let mut is_tail_write = block_on(self.shared_data.is_tail.write());
        *is_tail_write = is_tail;
        drop(is_tail_write);
    }

    pub fn set_new_tail(&mut self, new_tail: bool) {
        #[cfg(debug_assertions)]
        println!("Setting new tail status to: {}", new_tail);

        let mut new_tail_write = block_on(self.shared_data.new_tail.write());
        *new_tail_write = new_tail;
        drop(new_tail_write);
    }
}