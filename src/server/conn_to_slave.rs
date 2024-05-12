use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};

use crate::prelude::*;

use super::{
    cluster,
    codec::RespTcpStream,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum Message {
    Set(SetData),
    Refresh,
}

#[allow(unused)]
#[derive(DebugExtras)]
struct ConnToSlaveActor {
    #[debug_ignore]
    offset: usize,
    #[debug_ignore]
    slave_offset: usize,
    #[debug_ignore]
    cluster_hnd: cluster::ActorHandle,
    socket: SocketAddr,
    #[debug_ignore]
    stream: RespTcpStream,
    #[debug_ignore]
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl ConnToSlaveActor {
    async fn new(
        socket: SocketAddr,
        stream: RespTcpStream,
        cluster_hnd: cluster::ActorHandle,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            offset: 0,
            slave_offset: 0,
            cluster_hnd,
            socket,
            stream,
            receive,
        })
    }

    #[instrument(skip(self))]
    async fn propagate(&mut self, message: RedisMessage) -> anyhow::Result<()> {
        let message_offset = message.bytes_len();
        self.stream
            .send(message)
            .await
            .context("sent set to slave")?;
        self.offset += message_offset;
        Ok(())
    }

    #[instrument]
    async fn run(&mut self) -> anyhow::Result<()> {
        while let Ok(message) = self.receive.recv().await {
            info!(
                "received {:?}, offset: {}, client_offset: {}",
                message, self.offset, self.slave_offset
            );

            match message {
                Message::Set(set_data) => self.propagate(RedisMessage::Set(set_data)).await?,
                Message::Refresh => {
                    if self.offset == self.slave_offset {
                        tracing::info!("synced already");
                        continue;
                    }

                    self.stream.send(RedisMessage::ReplConfGetAck).await?;

                    match self.stream.next().await {
                        Some(Ok(RedisMessage::ReplConfAck { offset })) => {
                            self.slave_offset = offset;
                            tracing::info!(
                                "slave offset is {}, needs {}",
                                self.slave_offset,
                                self.offset
                            );

                            if self.offset == self.slave_offset {
                                tracing::info!("notify sync success");
                                self.cluster_hnd
                                    .send(cluster::Message::ReplicaSynced {
                                        socket: self.socket,
                                    })
                                    .await?;
                            }
                        }
                        t => {
                            self.stream
                                .send(RedisMessage::Err(format!("cannot process {:?}", t)))
                                .await?
                        }
                    }

                    self.offset += RedisMessage::ReplConfGetAck.bytes_len();
                }
            }

            trace!("slave processed message");
        }

        Ok(())
    }
}

pub struct ActorHandle {
    cluster_hnd: cluster::ActorHandle,
    broadcast: tokio::sync::broadcast::Sender<Message>,
}

#[allow(unused)]
// TODO: limit replicas
impl ActorHandle {
    pub fn new(
        cluster_hnd: cluster::ActorHandle,
        broadcast: tokio::sync::broadcast::Sender<Message>,
    ) -> Self {
        Self {
            cluster_hnd,
            broadcast,
        }
    }

    pub async fn run(&self, socket: SocketAddr, slave_stream: RespTcpStream) -> anyhow::Result<()> {
        let cluster_hnd = self.cluster_hnd.clone();
        let mut actor = ConnToSlaveActor::new(
            socket,
            slave_stream,
            cluster_hnd.clone(),
            self.broadcast.subscribe(),
        )
        .await?;
        tokio::spawn(async move {
            if let Err(err) = actor.run().await {
                error!("slave connection error: {err:?}");
                cluster_hnd
                    .send(cluster::Message::SlaveDisconnected { socket })
                    .await;
            }
        });

        Ok(())
    }

    pub async fn send(&self, message: Message) {
        match self.broadcast.send(message) {
            Ok(count) => info!("broadcasted to {count} replicas"),
            Err(_) => info!("no replicas connected"),
        }
    }
}
