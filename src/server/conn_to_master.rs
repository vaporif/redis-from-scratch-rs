use crate::{prelude::*, MasterAddr};
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::{
    codec::{RespCodec, RespTcpStream},
    commands::RedisMessage,
    error::TransportError,
    storage,
};

pub struct ConnToMasterActor {
    offset: usize,
    stream: RespTcpStream,
    storage_hnd: storage::ActorHandle,
}

impl ConnToMasterActor {
    pub async fn new(stream: TcpStream, storage_hnd: storage::ActorHandle) -> Self {
        let stream = Framed::new(stream, RespCodec);
        Self {
            offset: 0,
            storage_hnd,
            stream,
        }
    }

    #[instrument(skip(self))]
    pub async fn run(mut self, port: u16) -> anyhow::Result<()> {
        self.stream.send(RedisMessage::Ping(None)).await?;

        let RedisMessage::Pong = self
            .stream
            .next()
            .await
            .context("expecting repl pong response")?
            .context("expecting pong response")?
        else {
            bail!("expecting pong reply")
        };

        self.stream
            .send(RedisMessage::ReplConfPort { port })
            .await?;

        let RedisMessage::Ok = self
            .stream
            .next()
            .await
            .context("expecting repl ok response")?
            .context("expecting ok response")?
        else {
            bail!("expecting ok reply")
        };

        self.stream
            .send(RedisMessage::ReplConfCapa {
                capa: "psync2".to_string(),
            })
            .await
            .context("expecting send replconf")?;

        let RedisMessage::Ok = self
            .stream
            .next()
            .await
            .context("expecting repl response")?
            .context("expecting redis messsage")?
        else {
            bail!("expecting ok reply")
        };

        self.stream
            .send(RedisMessage::Psync {
                replication_id: "?".to_string(),
                offset: -1,
            })
            .await?;

        let RedisMessage::FullResync {
            replication_id: _,
            offset: _,
        } = self
            .stream
            .next()
            .await
            .context("expecting repl fullserync response")?
            .context("expecting fullresync resp")?
        else {
            bail!("expecting fullresync reply")
        };

        let expected_db = self.stream.next().await;

        match expected_db {
            Some(Ok(RedisMessage::DbTransfer(_))) => {
                info!("db received");
            }
            None => {
                info!("db not sent back");
            }
            Some(Err(error)) => Err(error)?,
            _ => bail!("incorrect response"),
        }

        trace!("connected to master");

        if let Err(error) = self.handle_master_connection().await {
            self.stream
                .send(RedisMessage::Err(format!("{:?}", error)))
                .await?;
            Err(error)?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn handle_master_connection(&mut self) -> anyhow::Result<(), TransportError> {
        while let Some(command) = self.stream.next().await {
            let command = command.context("reading master connection")?;

            let offset = command.bytes_len();

            info!("received command from master {:?}", &command);
            match command {
                RedisMessage::Set(data) => {
                    self.storage_hnd
                        .send(storage::Message::Set {
                            data,
                            channel: None,
                        })
                        .await
                        .context("sending set command")?;
                }
                RedisMessage::ReplConfGetAck => {
                    info!("sending offset {}", self.offset);
                    self.stream
                        .send(RedisMessage::ReplConfAck {
                            offset: self.offset,
                        })
                        .await
                        .context("sending replconf")?;
                }
                c => {
                    self.stream
                        .send(RedisMessage::Err(format!("cannot process {:?}", c)))
                        .await?
                }
            }

            self.offset += offset;
        }

        Ok(())
    }
}

#[allow(unused)]
pub fn spawn_actor(
    master_addr: MasterAddr,
    storage_hnd: storage::ActorHandle,
    port: u16,
    on_complete_tx: tokio::sync::oneshot::Sender<TcpStream>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            error!("replication crashed {info:?}");
            std::process::exit(1);
        }));

        let stream = tokio::net::TcpStream::connect(master_addr).await?;
        let actor = super::conn_to_master::ConnToMasterActor::new(stream, storage_hnd).await;
        actor.run(port).await
    })
}
