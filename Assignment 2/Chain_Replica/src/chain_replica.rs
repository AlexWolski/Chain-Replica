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

use tonic::{transport::Server, Request, Response, Status};
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


#[derive(Default)]
pub struct HeadChainReplicaService {}

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

#[derive(Default)]
pub struct TailChainReplicaService {}

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

#[derive(Default)]
pub struct ReplicaService {}

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


fn main() {
    println!("This is the replica");
}