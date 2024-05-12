use std::{collections::HashMap, default, net::SocketAddr, usize};

use super::conn_to_slave;
use rand::{distributions::Alphanumeric, Rng};

pub use crate::prelude::*;

use super::{codec::RespTcpStream, commands::SetData};

#[derive(Debug, Clone)]
pub enum ServerMode {
    Master(MasterInfo),
    Slave(MasterAddr),
}

#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub master_replid: String,
    pub master_repl_offset: usize,
}

impl ServerMode {
    fn new(replication_ip: Option<MasterAddr>) -> Self {
        match replication_ip {
            Some(master_addr) => ServerMode::Slave(master_addr),
            None => {
                let random_string: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40)
                    .map(char::from)
                    .collect();

                ServerMode::Master(MasterInfo {
                    master_replid: random_string,
                    master_repl_offset: 0,
                })
            }
        }
    }
}

pub type MasterAddr = (String, u16);
#[derive(DebugExtras)]
#[allow(unused)]
pub enum Message {
    AddNewSlave((SocketAddr, RespTcpStream)),
    SlaveDisconnected {
        socket: SocketAddr,
    },
    ReplicaSynced {
        socket: SocketAddr,
    },
    UpdateReplicas(SetData),
    RefreshAndGetReplicasSynced {
        expected: u64,
        #[debug_ignore]
        channel: tokio::sync::oneshot::Sender<u64>,
    },
    GetServerMode {
        #[debug_ignore]
        channel: tokio::sync::oneshot::Sender<ServerMode>,
    },
}

pub struct Actor {
    server_mode: ServerMode,
    replicas_updated: HashMap<SocketAddr, bool>,
    conn_to_slave_hnd: super::conn_to_slave::ActorHandle,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[allow(unused)]
impl Actor {
    fn new(
        master_addr: Option<MasterAddr>,
        conn_to_slave_hnd: super::conn_to_slave::ActorHandle,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
    ) -> Self {
        let random_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        Actor {
            server_mode: ServerMode::new(master_addr),
            replicas_updated: default::Default::default(),
            conn_to_slave_hnd,
            receiver,
        }
    }

    fn set_replica_sync_status(&mut self, socket: SocketAddr, synced: bool) {
        _ = self
            .replicas_updated
            .entry(socket)
            .and_modify(|f| *f = synced)
            .or_insert(synced)
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        trace!("cluster started {:?}", self.server_mode);
        while let Some(message) = self.receiver.recv().await {
            info!("received {:?}", message);
            match message {
                Message::ReplicaSynced { socket } => self.set_replica_sync_status(socket, true),
                Message::AddNewSlave((socket, mut stream)) => {
                    if let Err(error) = self.conn_to_slave_hnd.run(socket, stream).await {
                        error!("failed to connect {:?}", error);
                    }
                    self.set_replica_sync_status(socket, true);
                }
                Message::SlaveDisconnected { socket } => {
                    self.replicas_updated.remove(&socket);
                }
                Message::RefreshAndGetReplicasSynced { expected, channel } => {
                    info!("replicas updated count: {:?}", &self.replicas_updated);
                    let updated_replicas_count =
                        self.replicas_updated.values().filter(|f| **f).count();

                    if expected > updated_replicas_count as u64 {
                        self.conn_to_slave_hnd
                            .send(conn_to_slave::Message::Refresh)
                            .await;
                    }

                    channel.send(updated_replicas_count as u64).expect("sent");
                }
                Message::UpdateReplicas(set_data) => {
                    if let ServerMode::Master(_) = self.server_mode {
                        self.replicas_updated.values_mut().for_each(|f| *f = false);
                        info!("propagating command to slaves");
                        _ = self
                            .conn_to_slave_hnd
                            .send(conn_to_slave::Message::Set(set_data))
                            .await;
                    }
                }
                Message::GetServerMode { channel } => {
                    channel.send(self.server_mode.clone());
                }
            }
        }
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct ActorHandle {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
}

#[allow(unused)]
impl ActorHandle {
    pub fn new(replication_ip: Option<MasterAddr>) -> Self {
        let (broadcast, _) = tokio::sync::broadcast::channel(40);
        let (sender, receive) = tokio::sync::mpsc::unbounded_channel();
        let hnd = Self { sender };

        let handle = hnd.clone();

        tokio::spawn(async move {
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                error!("cluster crashed {info:?}");
                std::process::exit(1);
            }));
            let mut actor = Actor::new(
                replication_ip,
                super::conn_to_slave::ActorHandle::new(handle, broadcast),
                receive,
            );
            trace!("cluster actor started");
            loop {
                actor.run().await;
            }
        });

        hnd
    }

    #[instrument(skip(self))]
    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender
            .send(message)
            .context("[cluster] sending message")
    }
}
