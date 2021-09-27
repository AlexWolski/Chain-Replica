mod grpc_services;
use grpc_services::ReplicaData;

pub struct ServerManager {
    //Service data
    shared_data: Arc<ReplicaData>,
    is_paused: Arc<RwLock<bool>>,
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
            is_paused: Arc::new(RwLock::new(true)),
            shutdown_trigger: trigger,
            shutdown_listener: listener,
        })
    }
    
    pub fn start(&mut self, socket: SocketAddr, paused: bool) -> Result<(), Box<dyn std::error::Error>> {
        //Set the status of the server
        if paused {
            self.pause()?;
        }
        else {
            self.resume()?;
        }

        //Create the service struct
        let head_service = HeadChainReplicaService {
            shared_data: self.shared_data.clone(),
            is_paused: self.is_paused.clone(),
        };

        //Start the server
        let head_server = Server::builder().add_service(HeadChainReplicaServer::new(head_service));
        let listener = self.shutdown_listener.clone();

        let _ = tokio::spawn(async move {
            let _ = head_server.serve_with_shutdown(socket, listener).await;
        });

        Ok(())
    }

    pub fn stop(self) {
        self.shutdown_trigger.trigger();
    }

    pub fn pause(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Pausing head service");

        let mut is_paused_write = self.is_paused.write().unwrap();
        *is_paused_write = true;
        Ok(())
    }

    pub fn resume(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(debug_assertions)]
        println!("Resuming head service");
        
        let mut is_paused_write = self.is_paused.write().unwrap();
        *is_paused_write = false;
        Ok(())
    }
}