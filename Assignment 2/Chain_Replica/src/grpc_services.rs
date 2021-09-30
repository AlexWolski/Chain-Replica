//Import the rust file generated from the proto
pub mod chain {
    tonic::include_proto!("chain");
}

//Libraries
use std::collections::HashMap;
use std::net::SocketAddr;
use async_std::sync::{Arc, RwLock};
use tonic::{transport::Server, Request, Response, Status, Code};
use tokio::runtime::Handle;
use tokio::time::{sleep, Duration};
use tokio::sync::broadcast;
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
//The number of milliseconds to wait for ACKs to complete
static VAR_WAIT: u64 = 100;

pub struct ReplicaData {
    pub database: Arc<RwLock<HashMap<String, u32>>>,
    pub xid: Arc<RwLock<u32>>,
    pub last_ack: Arc<RwLock<u32>>,
    pub sent: Arc<RwLock<Vec<UpdateRequest>>>,
    pub my_addr: String,
    pub pred_addr: Arc<RwLock<Option<String>>>,
    pub succ_addr: Arc<RwLock<Option<String>>>,
    pub is_head: Arc<RwLock<bool>>,
    pub is_tail: Arc<RwLock<bool>>,
    pub new_tail: Arc<RwLock<bool>>,
    pub ack_event: broadcast::Sender<u32>,
}

impl ReplicaData {
    pub fn new(server_addr: String) -> ReplicaData  {
        ReplicaData{
            database: Arc::new(RwLock::new(HashMap::<String, u32>::new())),
            xid: Arc::new(RwLock::new(0)),
            last_ack: Arc::new(RwLock::new(0)),
            sent: Arc::new(RwLock::new(Vec::<UpdateRequest>::new())),
            my_addr: server_addr,
            //Assume that the replica is added to an empty chain
            pred_addr: Arc::new(RwLock::new(Option::<String>::None)),
            succ_addr: Arc::new(RwLock::new(Option::<String>::None)),
            is_head: Arc::new(RwLock::new(true)),
            is_tail: Arc::new(RwLock::new(true)),
            new_tail: Arc::new(RwLock::new(false)),
            ack_event: broadcast::channel(1).0,
        }
    }
}

pub struct HeadChainReplicaService {
    shared_data: Arc<ReplicaData>,
    tokio_rt: Handle,
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
            let new_xid = curr_xid + 1;
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
                xid: new_xid,
            });

            //Connect to the local replica service
            let uri = format!("https://{}", self.shared_data.my_addr);
            let mut replica_client = ReplicaClient::connect(uri).await.unwrap();

            //Send the UpdateRequest
            //No need to check for errors since its the local replica service
            self.tokio_rt.spawn(async move {
                let _ = replica_client.update(update_request).await;
            });

            //Keep listening for the ack with the correct xid
            loop {
                let mut ack_recv = self.shared_data.ack_event.subscribe();
                let ack_xid = ack_recv.recv().await.unwrap();

                if ack_xid == new_xid {
                    break;
                }
            }

            //Return an IncResponse once the ack is received
            let head_response = chain::HeadResponse { rc: 0 };
            Ok(Response::new(head_response))
        }
    }
}


pub struct TailChainReplicaService {
    shared_data: Arc<ReplicaData>,
    tokio_rt: Handle,
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
    tokio_rt: Handle,
}

impl ReplicaService {
    //Sent a state transfer to the successor server
    async fn send_state_transfer(shared_data: Arc<ReplicaData>, succ_addr: String) {
        loop {
            //Abort the state transfer if this is the tail
            let is_tail_read = shared_data.is_tail.read().await;
            if *is_tail_read { return }
            drop(is_tail_read);

            //Read the ip address of the current successor
            let curr_succ_addr_read = shared_data.succ_addr.read().await;
            let curr_succ_addr = (*curr_succ_addr_read).as_ref().unwrap().clone();
            drop(curr_succ_addr_read);

            //If the successor has changed, abort the transfer
            if curr_succ_addr != succ_addr {
                return
            }

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
                                0 => {
                                    #[cfg(debug_assertions)]
                                    println!("Successfully transfered state to successor at address: {}", succ_addr);
                                    return
                                },
                                1 => {
                                    #[cfg(debug_assertions)]
                                    println!("Successor at address: {} does not need a state transfer", succ_addr);
                                    return
                                },
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
    //This method must be created from within a tokio runtime
    async fn forward_update(shared_data: Arc<ReplicaData>, request: Request<UpdateRequest>, succ_addr: String) {
        let request_ref = request.get_ref();

        loop {
            //Discard the update if this is the tail
            let is_tail_read = shared_data.is_tail.read().await;
            if *is_tail_read { return };
            drop(is_tail_read);

            //Read the ip address of the current successor
            let curr_succ_addr_read = shared_data.succ_addr.read().await;
            let curr_succ_addr = (*curr_succ_addr_read).as_ref().unwrap().clone();
            drop(curr_succ_addr_read);

            //If the successor has changed, discard the update
            if curr_succ_addr != succ_addr {
                return
            }

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
                                0 => {
                                    #[cfg(debug_assertions)]
                                    println!("Successfully forwarded update to successor at address: {}", succ_addr);
                                    return
                                },
                                //State transfer requested. The successor has changed.
                                1 => {
                                    #[cfg(debug_assertions)]
                                    println!("State transfer request received from successor at address: {}", succ_addr);

                                    //Sent the state transfer and stop trying to send the update
                                    tokio::spawn(async move {
                                        ReplicaService::send_state_transfer(shared_data, succ_addr).await
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

    //Continuously try to send an ACK to the predecessor
    //If the predecessor changes, the ACKs will be sent to the new predecessor
    async fn send_ack(shared_data: Arc<ReplicaData>, request: Request<AckRequest>) {
        let request_ref = request.get_ref();

        loop {
            //Check if this replica is the head
            let is_head_read = shared_data.is_head.read().await;
            let is_head = *is_head_read;
            drop(is_head_read);

            //If this replica is the head, broadcast an ack event
            if is_head {
                shared_data.ack_event.send(request_ref.xid).unwrap();
                return
            }
            //If there is a predecessor, forward the ack
            else {
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

            //Ensure that the updates are processed in sequential order
            loop {
                //Get the updated xid
                let xid_read = self.shared_data.xid.read().await;
                let new_xid = *xid_read;
                drop(xid_read);

                //If the update is too new, wait for all previous updates to complete
                if new_xid > curr_xid + 1 {
                    sleep(Duration::from_millis(VAR_WAIT)).await;
                }
                else {
                    break;
                }
            }

            //Update  database, sent, and xid
            //Nest the locks to avoid conflict with state_transfer
            let mut data_write = self.shared_data.database.write().await;
            let mut sent_write = self.shared_data.sent.write().await;
            let mut xid_write = self.shared_data.xid.write().await;

            (*sent_write).push(request_ref.clone());
            let _ = (*data_write).insert(request_ref.key.clone(), request_ref.new_value as u32);
            *xid_write = request_ref.xid;

            drop(data_write);
            drop(xid_write);
            drop(sent_write);

            //Check if this replica is the tail
            let is_tail_read = self.shared_data.is_tail.read().await;
            let is_tail = *is_tail_read;
            drop(is_tail_read);

            let shared_data_clone = self.shared_data.clone();

            //If this replica is the tail, send an ack to the predecessor
            if is_tail {
                let ack_request = Request::<chain::AckRequest>::new(chain::AckRequest { xid: request_ref.xid });

                self.tokio_rt.spawn(async move {
                    ReplicaService::send_ack(shared_data_clone, ack_request).await
                });
            }
            //If there is a successor, forward the update
            else {
                //Read the ip address of the successor
                let succ_addr_read = self.shared_data.succ_addr.read().await;
                let succ_addr = (*succ_addr_read).as_ref().unwrap().clone();
                drop(succ_addr_read);

                self.tokio_rt.spawn(async move {
                    ReplicaService::forward_update(shared_data_clone, request, succ_addr).await
                });
            }

            //Return an UpdateResponse
            let update_response = chain::UpdateResponse { rc: 0 };
            Ok(Response::new(update_response))
        }
    }

    async fn state_transfer(&self, request: Request<StateTransferRequest>) ->
    Result<Response<StateTransferResponse>, Status> {
        let request_ref = request.get_ref();

        #[cfg(debug_assertions)]
        println!("Received StateTransferRequest ( xID: {} )", request_ref.xid);

        let mut new_tail_read = self.shared_data.new_tail.read().await;
        let new_tail = *new_tail_read;
        drop(new_tail_read);

        let mut data_write = self.shared_data.database.write().await;
        let mut xid_write = self.shared_data.xid.write().await;

        //If this is an empty replica, copy all of the data
        if new_tail {
            //Copy all of the replica data
            //Since this is a tail replica, don't copy the sent list
            //To-Do: Only send and copy the needed portions
            *data_write = request_ref.state.clone();
            *xid_write = request_ref.xid;

            //Set new_tail to false
            let mut new_tail_write = self.shared_data.new_tail.write().await;
            *new_tail_write = false;
            drop(new_tail_write);
        }
        //If this is not an empty replica, apply the new updates
        else {
            let curr_xid = *xid_write;
            let uri = format!("https://{}", self.shared_data.my_addr);
            let sent_list = request_ref.sent.to_owned();

            for update_request in sent_list {
                if update_request.xid > curr_xid {
                    let mut replica_client = ReplicaClient::connect(uri.clone()).await.unwrap();

                    //The update threads won't complete until the locks are released
                    self.tokio_rt.spawn(async move {
                        let _ = replica_client.update(update_request).await;
                    });
                } 
            }

            //If the predecessor is missing any ACKs, forward them
            let mut missing_xid = request_ref.xid;

            while missing_xid < *xid_write {
                let shared_data_clone = self.shared_data.clone();
                let ack_request = Request::<chain::AckRequest>::new(chain::AckRequest { xid: missing_xid });

                self.tokio_rt.spawn(async move {
                    ReplicaService::send_ack(shared_data_clone, ack_request).await
                });

                missing_xid += 1;
            }
        }

        drop(xid_write);
        drop(data_write);

        let transfer_response = chain::StateTransferResponse { rc: 0 };
        Ok(Response::new(transfer_response))
    }

    async fn ack(&self, request: Request<AckRequest>) ->
    Result<Response<AckResponse>, Status> {
        #[cfg(debug_assertions)]
        let ack_xid = request.get_ref().xid;
        println!("Received AckRequest ( xID: {} )", ack_xid);

        //Ensure that ACKs are processed in sequential order
        loop {
            //Get the last ack received
            let last_ack_read = self.shared_data.last_ack.read().await;
            let last_ack = *last_ack_read;
            drop(last_ack_read);

            //If the ack is too new, wait for all previous acks to complete
            if ack_xid > last_ack + 1 {
                sleep(Duration::from_millis(VAR_WAIT)).await;
            }
            else {
                break;
            }
        }

        let sent_read = self.shared_data.sent.read().await;
        let sent: &Vec<UpdateRequest> = (*sent_read).as_ref();
        let len = sent.len();
        let mut index = 0;

        //Find the update correspoinding to the xid
        for update in sent {
            if update.xid == ack_xid {
                break;
            }

            index += 1;
        }

        drop(sent_read);

        //Lock sent and last_ack to prevent conflicts
        let mut sent_write = self.shared_data.sent.write().await;
        let mut last_ack_write = self.shared_data.last_ack.write().await;

        //If the update was in the sent list, remove it
        if index < len {
            (*sent_write).remove(index);
        }

        let shared_data_clone = self.shared_data.clone();

        //Forward the ack to the predecessor
        self.tokio_rt.spawn(async move {
            ReplicaService::send_ack(shared_data_clone, request).await
        });

        //Update the last ack processed
        *last_ack_write = ack_xid;

        drop(last_ack_write);
        drop(sent_write);

        Ok(Response::new(chain::AckResponse {}))
    }
}


pub struct ServerManager {
    //Service data
    shared_data: Arc<ReplicaData>,
    //Server data
    shutdown_trigger: Trigger,
    shutdown_listener: Listener,
    //Tokio runtime environment
    tokio_rt: Handle,
}

impl ServerManager {
    pub fn new(shared_data: Arc<ReplicaData>, tokio_rt: Handle) -> ServerManager  {
        let (trigger, listener) = triggered::trigger();

        ServerManager {
            shared_data: shared_data.clone(),
            shutdown_trigger: trigger,
            shutdown_listener: listener,
            tokio_rt: tokio_rt.clone(),
        }
    }
    
    pub fn start(&mut self, socket: SocketAddr, pred_addr: Option<String>, succ_addr: Option<String>) {
        //If there is a predecessor but no successor, set new_tail to indicate a state transfer is needed
        if pred_addr.is_some() && succ_addr.is_none() {
            let mut new_tail_write = block_on(self.shared_data.new_tail.write());
            *new_tail_write = true;
            drop(new_tail_write);
        }

        self.set_pred(pred_addr);
        self.set_succ(succ_addr);

        //Create the service structs
        let head_service = HeadChainReplicaService { shared_data: self.shared_data.clone(), tokio_rt: self.tokio_rt.clone() };
        let tail_service = TailChainReplicaService { shared_data: self.shared_data.clone(), tokio_rt: self.tokio_rt.clone() };
        let replica_service = ReplicaService { shared_data: self.shared_data.clone(), tokio_rt: self.tokio_rt.clone() };

        //Add all three services to a server
        let head_server = Server::builder()
            .add_service(HeadChainReplicaServer::new(head_service))
            .add_service(TailChainReplicaServer::new(tail_service))
            .add_service(ReplicaServer::new(replica_service));

        //Start the server
        let listener = self.shutdown_listener.clone();

        let _ = self.tokio_rt.spawn(async move {
            let _ = head_server.serve_with_shutdown(socket, listener).await;
        });
    }

    pub fn stop(self) {
        self.shutdown_trigger.trigger();
    }

    pub fn set_pred(&mut self, pred_addr: Option<String>) {
        //Get current predecessor
        let pred_addr_read = block_on(self.shared_data.pred_addr.read());
        let curr_pred_addr = pred_addr_read.clone();
        drop(pred_addr_read);

        //Check if the predecessor changed
        if curr_pred_addr != pred_addr.clone() {
            #[cfg(debug_assertions)]
            println!("Setting predecessor to: {:?}", pred_addr);

            //Set the predecessor
            let mut pred_addr_write = block_on(self.shared_data.pred_addr.write());
            *pred_addr_write = pred_addr.clone();
            drop(pred_addr_write);

            //Set is_head
            match pred_addr {
                Some(addr) => self.set_head(false),
                None => self.set_head(true),
            }
        };
    }

    pub fn set_succ(&mut self, succ_addr: Option<String>) {
        //Get current succecessor
        let succ_addr_read = block_on(self.shared_data.succ_addr.read());
        let curr_succ_addr = succ_addr_read.clone();
        drop(succ_addr_read);

        //Check if the succecessor changed
        if curr_succ_addr != succ_addr.clone() {
            #[cfg(debug_assertions)]
            println!("Setting succecessor to: {:?}", succ_addr);

            //Set the succecessor
            let mut succ_addr_write = block_on(self.shared_data.succ_addr.write());
            *succ_addr_write = succ_addr.clone();
            drop(succ_addr_write);

            //Set is_tail
            match succ_addr.clone() {
                Some(addr) => {
                    self.set_tail(false);

                    //Send a state transfer
                    #[cfg(debug_assertions)]
                    println!("Transfering state to new successor at address: {}", addr);
                    let shared_data_clone = self.shared_data.clone();

                    self.tokio_rt.spawn(async move {
                        ReplicaService::send_state_transfer(shared_data_clone, addr).await
                    });
                },
                None => self.set_tail(true),
            }
        };
    }

    fn set_head(&mut self, is_head: bool) {
        let mut is_head_write = block_on(self.shared_data.is_head.write());
        *is_head_write = is_head;
        drop(is_head_write);
    }

    fn set_tail(&mut self, is_tail: bool) {
        let mut is_tail_write = block_on(self.shared_data.is_tail.write());
        *is_tail_write = is_tail;
        drop(is_tail_write);
    }
}