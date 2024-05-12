use std::net::SocketAddr;

use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;
use tracing::Instrument;

use super::{
    cluster::{self, ServerMode},
    codec::{RespCodec, RespTcpStream},
    commands::*,
    error::TransportError,
    storage,
};
use crate::{
    prelude::*,
    server::{cluster::MasterInfo, rdb::Rdb},
    ExecutorMessenger,
};

#[allow(unused)]
#[derive(DebugExtras)]
pub struct ConnectionActor {
    #[debug_ignore]
    executor_messenger: ExecutorMessenger,
    #[debug_ignore]
    storage_hnd: storage::ActorHandle,
    #[debug_ignore]
    cluster_hnd: cluster::ActorHandle,
    socket: SocketAddr,
    #[debug_ignore]
    stream: RespTcpStream,
}

#[allow(unused)]
enum ConnectionResult {
    Dropped,
    SwitchToSlaveMode,
}

impl ConnectionActor {
    pub fn new(
        executor_messenger: ExecutorMessenger,
        storage_hnd: storage::ActorHandle,
        cluster_hnd: cluster::ActorHandle,
        stream_socket: SocketAddr,
        stream: TcpStream,
    ) -> Self {
        let stream = Framed::new(stream, RespCodec);
        Self {
            storage_hnd,
            cluster_hnd,
            executor_messenger,
            socket: stream_socket,
            stream,
        }
    }

    #[instrument]
    pub async fn run(mut self) -> anyhow::Result<()> {
        trace!("retrieving new message from client");
        match self.handle_connection().await? {
            ConnectionResult::SwitchToSlaveMode => {
                self.cluster_hnd
                    .send(cluster::Message::AddNewSlave((self.socket, self.stream)))
                    .await
                    .context("sending add new slave")?;
                Ok(())
            }
            ConnectionResult::Dropped => self.stream.close().await.context("closing stream"),
        }
    }

    #[instrument(skip(self))]
    async fn get_up_to_date_replica_count(&mut self, expected: u64) -> anyhow::Result<u64> {
        let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
        self.cluster_hnd
            .send(cluster::Message::RefreshAndGetReplicasSynced {
                channel: reply_channel_tx,
                expected,
            })
            .await
            .context("get slave count")?;

        let count = reply_channel_rx
            .await
            .context("waiting for reply for wait")?;

        trace!("got {count} synced replicas");

        Ok(count)
    }

    async fn get_server_mode(&mut self) -> anyhow::Result<ServerMode> {
        let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
        self.cluster_hnd
            .send(cluster::Message::GetServerMode {
                channel: reply_channel_tx,
            })
            .await?;

        reply_channel_rx
            .await
            .context("waiting for get server mode")
    }

    async fn handle_connection(&mut self) -> Result<ConnectionResult, TransportError> {
        loop {
            match self.stream.next().await {
                Some(command) => {
                    let command = command?;

                    let span = span!(Level::INFO, "processing command", command = ?&command);
                    let slave = async {
                        match command {
                            RedisMessage::Err(_) => self.stream.send(command).await?,
                            RedisMessage::Ping(_) => self.stream.send(RedisMessage::Pong).await?,
                            RedisMessage::Echo(echo_string) => {
                                self.stream
                                    .send(RedisMessage::EchoResponse(echo_string))
                                .await?
                            }
                            RedisMessage::Wait {
                                req_replica_count,
                                timeout,
                            } => {
                                let mut replica_count =
                                self.get_up_to_date_replica_count(req_replica_count).await?;

                                if timeout == 0 || replica_count >= req_replica_count {
                                    self.stream
                                        .send(RedisMessage::WaitReply { replica_count })
                                    .await?;
                                } else {
                                    let interval_duration =
                                    tokio::time::Duration::from_millis(timeout / 2);
                                    let start_instant = tokio::time::Instant::now() + interval_duration;
                                    let mut interval =
                                    tokio::time::interval_at(start_instant, interval_duration);
                                    tokio::pin! {
                                        let timeout_fut =
                                        tokio::time::sleep(tokio::time::Duration::from_millis(timeout));
                                    }

                                    loop {
                                        // Both are cancel-safe as tcp is read via framed wrapper
                                        tokio::select! {
                                            _ = &mut timeout_fut => {
                                                tracing::trace!("[TIMEOUT] returning synced {} replicas", replica_count);
                                                self.stream.send(RedisMessage::WaitReply { replica_count}).await?;
                                                break;
                                            }
                                            _ = interval.tick() => {
                                                replica_count =
                                                    self.get_up_to_date_replica_count(req_replica_count).await?;
                                                tracing::trace!("[TICK RETRY] got {} replicas", replica_count);
                                                if replica_count >= req_replica_count {
                                                    self.stream.send(RedisMessage::WaitReply { replica_count }).await?;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            RedisMessage::Set(data) => {
                                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                                self.storage_hnd
                                    .send(storage::Message::Set {
                                        data,
                                        channel: Some(reply_channel_tx),
                                    })
                                    .await
                                    .context("sending set store command")?;

                                let message =
                                match reply_channel_rx.await.context("waiting for reply for set") {
                                    Ok(_) => RedisMessage::Ok,
                                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                                };

                                self.stream.send(message).await?
                            }
                            RedisMessage::ReplConfCapa { .. } => {
                                self.stream.send(RedisMessage::Ok).await?
                            }
                            RedisMessage::ReplConfPort { .. } => {
                                self.stream.send(RedisMessage::Ok).await?
                            }
                            RedisMessage::Psync {
                                replication_id,
                                offset,
                            } => match (replication_id.as_str(), offset) {
                                    ("?", -1) => {
                                        let server_mode = self.get_server_mode().await?;
                                        let ServerMode::Master(MasterInfo {
                                        ref master_replid, ..
                                    }) = server_mode
                                        else {
                                            return Err(TransportError::Other(anyhow!(
                                            "server in slave mode".to_string()
                                        )));
                                        };

                                        let resync_msq = RedisMessage::FullResync {
                                            replication_id: master_replid.clone(),
                                            offset: 0,
                                        };

                                        self.stream.send(resync_msq).await?;

                                        let db = Rdb::empty();
                                        let db_message = RedisMessage::DbTransfer(db.to_vec());

                                        self.stream.send(db_message).await?;

                                        return Ok(true);
                                    }
                                    _ => todo!(),
                                },
                            RedisMessage::Get(key) => {
                                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                                self.storage_hnd
                                    .send(storage::Message::Get {
                                        key,
                                        channel: reply_channel_tx,
                                    })
                                    .await
                                    .context("sending set store command")?;

                                let message =
                                match reply_channel_rx.await.context("waiting for reply for get") {
                                    Ok(result) => match result {
                                        Some(value) => RedisMessage::CacheFound(value.into_bytes()),
                                        None => RedisMessage::CacheNotFound,
                                    },
                                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                                };

                                self.stream.send(message).await?
                            }
                            RedisMessage::Info(info_data) => match info_data {
                                InfoCommand::Replication => {
                                    let server_mode = self.get_server_mode().await?;
                                    self.stream
                                        .send(RedisMessage::InfoResponse(server_mode.clone()))
                                    .await?
                                }
                            },
                            _ => {
                                self.stream
                                    .send(RedisMessage::Err(
                                        "command could not be processed".to_string(),
                                    ))
                                .await?
                            }
                        }

                        Ok(false)
                    }
                    .instrument(span)
                    .await?;

                    if slave {
                        return Ok(ConnectionResult::SwitchToSlaveMode);
                    }
                }
                None => return Ok(ConnectionResult::Dropped),
            }
        }
    }
}

pub fn spawn_actor(
    socket: SocketAddr,
    stream: TcpStream,
    executor_messenger: ExecutorMessenger,
    storage_hnd: storage::ActorHandle,
    cluster_hnd: cluster::ActorHandle,
) {
    tokio::spawn(async move {
        let actor = super::conn::ConnectionActor::new(
            executor_messenger,
            storage_hnd,
            cluster_hnd,
            socket,
            stream,
        );
        if let Err(err) = actor.run().await {
            error!("connection failure: {:?}", err);
        }

        trace!("generic redis connection stopped");
    });
}
