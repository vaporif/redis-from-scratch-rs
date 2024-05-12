use std::net::SocketAddr;

use crate::{prelude::*, MasterAddr};
use tokio::{net::TcpStream, task::JoinHandle};
use tracing::{trace_span, Instrument};

pub async fn spawn_actor_executor(
    replication_ip: Option<MasterAddr>,
    port: u16,
    max_connections: usize,
) -> (ExecutorMessenger, JoinHandle<anyhow::Result<()>>) {
    let (connection_sender, connection_receiver) = tokio::sync::mpsc::channel(max_connections);
    let (internal_sender, internal_receiver) = tokio::sync::mpsc::channel(max_connections);

    let executor_messenger = ExecutorMessenger {
        connection_sender,
        internal_sender,
    };

    let resp_executor_messenger = executor_messenger.clone();

    let is_slave = replication_ip.is_some();

    let join_handle = tokio::spawn(async move {
        let executor = Executor::new(
            replication_ip,
            port,
            executor_messenger,
            connection_receiver,
            internal_receiver,
        )
        .await;

        match executor {
            Ok(executor) => {
                let span = if is_slave {
                    trace_span!("SLAVE MODE")
                } else {
                    trace_span!("MASTER MODE")
                };
                executor.run().instrument(span).await?;
            }
            Err(err) => Err(err)?,
        }

        Ok(())
    });

    (resp_executor_messenger, join_handle)
}

#[derive(Clone)]
pub struct ExecutorMessenger {
    pub connection_sender: tokio::sync::mpsc::Sender<ConnectionMessage>,
    internal_sender: tokio::sync::mpsc::Sender<Message>,
}

impl ExecutorMessenger {
    pub async fn propagate_fatal_errors<T>(&self, result: anyhow::Result<T>) {
        if let Err(error) = result {
            self.internal_sender
                .send(Message::FatalError(error))
                .await
                .expect("should send error")
        }
    }

    pub async fn publish_msg(&self, msg: Message) -> anyhow::Result<()> {
        self.internal_sender
            .send(msg)
            .await
            .context("could not send forwarding req")
    }
}

#[allow(unused)]
pub struct Executor {
    port: u16,
    storage_hnd: super::storage::ActorHandle,
    cluster_hnd: super::cluster::ActorHandle,
    executor_messenger: ExecutorMessenger,
    connection_receiver: tokio::sync::mpsc::Receiver<ConnectionMessage>,
    internal_receiver: tokio::sync::mpsc::Receiver<Message>,
}

impl Executor {
    async fn new(
        replication_ip: Option<MasterAddr>,
        port: u16,
        executor_messenger: ExecutorMessenger,
        connection_receiver: tokio::sync::mpsc::Receiver<ConnectionMessage>,
        internal_receiver: tokio::sync::mpsc::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        let cluster_hnd = super::cluster::ActorHandle::new(replication_ip.clone());
        let storage_hnd = super::storage::ActorHandle::new(cluster_hnd.clone());

        if let Some(master_addr) = replication_ip {
            trace!("replicating");
            let (db_done_tx, _) = tokio::sync::oneshot::channel();

            executor_messenger
                .internal_sender
                .send(Message::ReplicateMaster {
                    socket: master_addr,
                    channel: db_done_tx,
                })
                .await
                .context("replicate from master")?;

            // TODO: exercise doesn't reply :(
            // db_done_rx
            //     .await
            //     .context("waiting till master replica is completed")?;
        }

        Ok(Self {
            port,
            storage_hnd,
            cluster_hnd,
            executor_messenger,
            connection_receiver,
            internal_receiver,
        })
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
             Some(message) = self.connection_receiver.recv() => {
                    trace!("executor message received {:?}", &message);
                    match message {
                        ConnectionMessage::NewConnection((stream, socket)) => {
                            let storage_hnd = self.storage_hnd.clone();
                            let executor_messenger = self.executor_messenger.clone();
                            let cluster_hnd = self.cluster_hnd.clone();

                            super::conn::spawn_actor(socket, stream, executor_messenger, storage_hnd, cluster_hnd)

                        }
                        ConnectionMessage::FatalError(error) => bail!(format!("fatal error {:?}", error)),
                    }
                }
             Some(message) = self.internal_receiver.recv() => {
                    trace!("executor message received {:?}", &message);
                    match message {
                        Message::ReplicateMaster { ref socket, channel } => {
                            let master_addr = socket.clone();
                            super::conn_to_master::spawn_actor(master_addr, self.storage_hnd.clone(), self.port, channel);
                        },
                        Message::FatalError(error) => Err(error)?
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ConnectionMessage {
    NewConnection((tokio::net::TcpStream, SocketAddr)),
    FatalError(std::io::Error),
}

#[derive(DebugExtras)]
pub enum Message {
    ReplicateMaster {
        socket: MasterAddr,
        #[debug_ignore]
        channel: tokio::sync::oneshot::Sender<TcpStream>,
    },
    FatalError(anyhow::Error),
}
