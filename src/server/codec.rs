use super::{
    cluster::{MasterInfo, ServerMode},
    error::TransportError,
};
use bytes::Buf;
use itertools::{Either, Itertools};
use lazy_static::lazy_static;
use serde_resp::{array, bulk, bulk_null, de, err_str, ser, simple, RESP};
use std::{
    io::{BufReader, Cursor},
    time::Duration,
    usize,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::commands::*;
use crate::prelude::*;

lazy_static! {
    static ref UNKNOWN_COMMAND: RedisMessage = RedisMessage::Err(String::from("unknown command"));
}

#[derive(Debug)]
pub struct RespCodec;

pub type RespTcpStream = Framed<tokio::net::TcpStream, RespCodec>;

impl Decoder for RespCodec {
    type Item = RedisMessage;

    type Error = TransportError;

    #[instrument]
    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let cursor = Cursor::new(src.clone().freeze());
        let mut buff_reader = BufReader::new(cursor);
        trace!("attempting decode");
        let message: serde_resp::Result<RESP> = de::from_buf_reader(&mut buff_reader);
        match message {
            Ok(message) => {
                let resp = RESP::from(message.clone());
                let len_read = ser::to_string(&resp)
                    .context("would not fail")?
                    .bytes()
                    .len();
                trace!("ADVANCE {} pos", &len_read);

                if src.len() < len_read {
                    Err(anyhow!("WTF1???"))?;
                }

                src.advance(len_read);
                trace!("left bytes {:?}", &src);
                let Ok(message) = RedisMessage::try_from(message) else {
                    return Ok(Some(UNKNOWN_COMMAND.clone()));
                };

                trace!("received {:?}", &message);
                Ok(Some(message))
            }
            Err(error) => match error {
                serde_resp::Error::Eof => Ok(try_get_db_redis_msg(src)?),
                serde_resp::Error::Syntax => Ok(try_get_db_redis_msg(src)?),
                e => Err(anyhow!(e).into()),
            },
        }
    }
}

// DB redis message is not correct resp string, enjoy manual binary parse
#[instrument(skip_all)]
fn try_get_db_redis_msg(src: &mut bytes::BytesMut) -> Result<Option<RedisMessage>> {
    trace!("attempting db decode");
    if b'$' != src.get_u8() {
        Err(serde_resp::Error::Syntax)?
    }

    let db_len = match get_resp_int(src) {
        Ok(Some(db_len)) => {
            trace!("got number {db_len:?}");
            if src.len() < 2 {
                return Ok(None);
            }

            let mut crlf = [0; 2];
            src.copy_to_slice(&mut crlf);
            if crlf[0] != b'\r' || crlf[1] != b'\n' {
                Err(anyhow!("no crf in probable db response after numbers"))?
            }
            db_len
        }
        Ok(None) => return Ok(None),
        Err(err) => {
            return Err(anyhow!(
                "failed to parse number in probable db response {err:?}"
            ))?
        }
    };

    if src.len() < db_len {
        return Ok(None);
    }

    let db = src.split_to(db_len);
    let db_msg = RedisMessage::DbTransfer(db.to_vec());

    trace!("parsed as {db_msg:?}");
    return Ok(Some(db_msg));

    fn get_resp_int(src: &mut bytes::BytesMut) -> Result<Option<usize>> {
        let mut int_vec = Vec::new();

        for byte in src.iter() {
            if *byte == b'\r' {
                let len = int_vec.len();

                let integer = String::from_utf8(int_vec)
                    .context("utf8 expected as char for int")?
                    .parse::<usize>()
                    .context("failed to parse")?;
                if src.len() < len {
                    bail!("WTF2???");
                }
                src.advance(len);
                return Ok(Some(integer));
            }

            int_vec.push(*byte);
        }

        trace!("no crlf in number search");
        Ok(None)
    }
}

impl Encoder<RedisMessage> for RespCodec {
    type Error = anyhow::Error;

    #[instrument(skip(self, dst))]
    fn encode(
        &mut self,
        item: RedisMessage,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        trace!("writing message {:?}", item);
        let message_bytes = match item {
            RedisMessage::DbTransfer(db) => {
                let mut data = format!("${}\r\n", db.len()).as_bytes().to_vec();
                trace!("serialized db transfer");
                data.extend(db);
                data
            }
            item => {
                let item: RESP = item.into();
                let serialized = ser::to_string(&item).context("serialize resp")?;

                trace!("serialized as {:?}", &serialized);
                serialized.into_bytes()
            }
        };

        dst.extend_from_slice(&message_bytes);

        Ok(())
    }
}

impl TryFrom<RESP> for RedisMessage {
    type Error = super::error::TransportError;

    fn try_from(message: RESP) -> std::prelude::v1::Result<Self, Self::Error> {
        match message {
            serde_resp::RESPType::SimpleString(string) => {
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Error(error) => {
                Err(TransportError::ResponseError(error.to_string()))
            }
            serde_resp::RESPType::Integer(_) => Err(TransportError::Other(anyhow!(
                "integer not expected".to_string()
            ))),
            serde_resp::RESPType::BulkString(bytes) => {
                let bytes = bytes.context("empty bytes")?;
                let string = resp_bulkstring_to_string(bytes)?;
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Array(data) => {
                let Some(data) = data else {
                    return Err(TransportError::Other(anyhow!("array empty".to_string())));
                };

                RedisMessage::parse_array_message(data)
            }
        }
    }
}

fn resp_bulkstring_to_string(bytes: Vec<u8>) -> anyhow::Result<String> {
    String::from_utf8(bytes).context("building utf8 string from resp bulkstring")
}

struct RedisArrayCommand {
    command: String,
    args: Vec<String>,
}

impl RedisArrayCommand {
    fn from(resp: Vec<RESP>) -> anyhow::Result<Self> {
        let mut resp = resp.into_iter().map(RedisArrayCommand::map_string);

        let command = resp.next().context("getting command")??;
        let (args, errs): (Vec<String>, Vec<anyhow::Error>) =
            resp.partition_map(|result| match result {
                Ok(ok) => Either::Left(ok),
                Err(err) => Either::Right(err),
            });

        // TODO: Helper
        let error: Vec<_> = errs.into_iter().map(|f| f.to_string()).collect();
        if !error.is_empty() {
            bail!(error.join("\n"));
        }

        Ok(RedisArrayCommand { command, args })
    }

    fn map_string(resp: RESP) -> anyhow::Result<String> {
        let RESP::BulkString(Some(bytes)) = resp else {
            bail!("non bulk string for command");
        };

        resp_bulkstring_to_string(bytes)
    }
}

impl RedisMessage {
    #[instrument]
    fn parse_string_message(message: &str) -> anyhow::Result<RedisMessage, TransportError> {
        match message.to_lowercase().as_str() {
            "ping" => Ok(RedisMessage::Ping(None)),
            "pong" => Ok(RedisMessage::Pong),
            "ok" => Ok(RedisMessage::Ok),
            s if s.starts_with("fullresync") => {
                let mut split_message = s.split_whitespace().skip(1);
                let replication_id = split_message
                    .next()
                    .context("replication_id missed")?
                    .to_string();
                let offset = split_message
                    .next()
                    .context("offset missed")?
                    .parse::<i32>()
                    .context("wrong data")?;
                Ok(RedisMessage::FullResync {
                    replication_id,
                    offset,
                })
            }
            _ => Ok(UNKNOWN_COMMAND.clone()),
        }
    }

    // TODO: swap_remove instead of clone?
    // break into subfunctions
    #[instrument]
    fn parse_array_message(messages: Vec<RESP>) -> Result<RedisMessage, TransportError> {
        let array_command = RedisArrayCommand::from(messages)?;

        let command = array_command.command.to_uppercase();
        match command.as_str() {
            "PING" => Ok(RedisMessage::Ping(None)),
            "ECHO" => {
                let arg = array_command.args.first().context("echo command arg")?;

                Ok(RedisMessage::Echo(arg.clone()))
            }
            "SET" => {
                let key = array_command.args.first().context("set key arg")?;
                let value = array_command.args.get(1).context("set value arg")?;

                let mut set_data = SetData {
                    key: key.clone(),
                    value: value.clone(),
                    arguments: SetArguments { ttl: None },
                };

                // TODO: there are other args!
                if let Some(ttl_format) = array_command.args.get(2) {
                    let ttl = array_command.args.get(3).context("set ttl")?;
                    let ttl = ttl.parse::<u64>().context("converting ttl to number")?;

                    match ttl_format.to_uppercase().as_ref() {
                        "EX" => {
                            set_data.arguments.ttl = Some(Duration::from_secs(ttl));
                        }
                        "PX" => {
                            set_data.arguments.ttl = Some(Duration::from_millis(ttl));
                        }
                        _ => Err(TransportError::UnknownCommand)?,
                    }
                }

                Ok(RedisMessage::Set(set_data))
            }
            "GET" => {
                let key = array_command.args.first().context("get key arg")?;

                Ok(RedisMessage::Get(key.clone()))
            }
            "WAIT" => {
                let req_replica_count =
                    array_command.args.first().context("wait replicacoun arg")?;
                let req_replica_count = req_replica_count.parse().context("parse replica count")?;

                let timeout = array_command.args.get(1).context("wait replicacoun arg")?;
                let timeout = timeout.parse().context("parse replica count")?;

                Ok(RedisMessage::Wait {
                    req_replica_count,
                    timeout,
                })
            }
            "REPLCONF" => {
                let subcommand = array_command
                    .args
                    .first()
                    .context("replconf subcommand arg")?;

                match subcommand.to_uppercase().as_str() {
                    "LISTENING-PORT" => {
                        let port = array_command.args.get(1).context("replconf port")?;
                        let port = port.parse().context("parse port")?;
                        Ok(RedisMessage::ReplConfPort { port })
                    }
                    "CAPA" => {
                        let capa = array_command
                            .args
                            .get(1)
                            .context("replconf capa")?
                            .to_string();

                        Ok(RedisMessage::ReplConfCapa { capa })
                    }
                    "GETACK" => {
                        let offset = array_command
                            .args
                            .get(1)
                            .context("replconf get ack offset")?
                            .to_string();

                        if offset != "*" {
                            Err(anyhow::anyhow!("should have * as offset"))?
                        }

                        Ok(RedisMessage::ReplConfGetAck)
                    }
                    "ACK" => {
                        let offset = array_command
                            .args
                            .get(1)
                            .context("replconf ack offset")?
                            .to_string();

                        let offset = offset.parse().context("parse offset")?;

                        Ok(RedisMessage::ReplConfAck { offset })
                    }
                    s => Err(anyhow::anyhow!("unknown replconf {:?}", s))?,
                }
            }
            "PSYNC" => {
                let replication_id = array_command
                    .args
                    .first()
                    .context("psync repl_id")?
                    .to_string();
                let offset = array_command.args.get(1).context("psync offset")?;
                let offset = offset.parse().context("psync offset parse")?;
                Ok(RedisMessage::Psync {
                    replication_id,
                    offset,
                })
            }
            "INFO" => {
                let subcommand = array_command.args.first().context("info subcommand")?;

                let subcommand = subcommand.to_uppercase();
                match subcommand.as_str() {
                    "REPLICATION" => Ok(RedisMessage::Info(InfoCommand::Replication)),
                    _ => Err(TransportError::UnknownCommand),
                }
            }
            _ => Err(TransportError::UnknownCommand),
        }
    }
}

impl From<RedisMessage> for RESP {
    fn from(val: RedisMessage) -> Self {
        match val {
            RedisMessage::Ping(_) => array![bulk!(b"PING".to_vec())],
            RedisMessage::Pong => simple!("PONG".to_string()),
            RedisMessage::Echo(echo_string) => {
                array![bulk!(echo_string.bytes().collect_vec())]
            }
            RedisMessage::Set(set_data) => {
                array![
                    bulk!(b"SET".to_vec()),
                    bulk!(set_data.key.into_bytes()),
                    bulk!(set_data.value.into_bytes())
                ]
            }
            RedisMessage::Get(key) => {
                array![bulk!(b"GET".to_vec()), bulk!(key.bytes().collect_vec()),]
            }
            RedisMessage::ReplConfPort { port } => array![
                bulk!(b"REPLCONF".to_vec()),
                bulk!(b"listening-port".to_vec()),
                bulk!(port.to_string().into_bytes()),
            ],
            RedisMessage::ReplConfCapa { capa } => array![
                bulk!(b"REPLCONF".to_vec()),
                bulk!(b"CAPA".to_vec()),
                bulk!(capa.to_string().into_bytes()),
            ],
            RedisMessage::Info(_) => {
                array![bulk!(b"INFO".to_vec()), bulk!(b"REPLICATION".to_vec()),]
            }
            RedisMessage::Ok => bulk!(b"OK".to_vec()),
            RedisMessage::Psync {
                replication_id,
                offset,
            } => array![
                bulk!(b"PSYNC".to_vec()),
                bulk!(replication_id.into_bytes()),
                bulk!(offset.to_string().into_bytes()),
            ],
            RedisMessage::FullResync {
                replication_id,
                offset,
            } => simple!(format!("FULLRESYNC {replication_id} {offset}")),
            RedisMessage::EchoResponse(echo) => bulk!(echo.into_bytes()),
            RedisMessage::Err(error) => err_str!(error),
            RedisMessage::CacheFound(val) => bulk!(val),
            RedisMessage::CacheNotFound => bulk_null!(),
            RedisMessage::InfoResponse(server_mode) => server_mode.to_resp(),
            RedisMessage::DbTransfer(_) => {
                panic!("db transfer is not proper resp messag")
            }
            RedisMessage::ReplConfGetAck => {
                array![
                    bulk!(b"REPLCONF".to_vec()),
                    bulk!(b"GETACK".to_vec()),
                    bulk!(b"*".to_vec())
                ]
            }
            RedisMessage::ReplConfAck { offset } => {
                array![
                    bulk!(b"REPLCONF".to_vec()),
                    bulk!(b"ACK".to_vec()),
                    bulk!(offset.to_string().bytes().collect_vec())
                ]
            }
            RedisMessage::Wait {
                req_replica_count: replica_count,
                timeout,
            } => {
                array![
                    bulk!(b"WAIT".to_vec()),
                    bulk!(replica_count.to_string().bytes().collect_vec()),
                    bulk!(timeout.to_string().bytes().collect_vec())
                ]
            }
            RedisMessage::WaitReply { replica_count } => serde_resp::int!(replica_count as i64),
        }
    }
}

impl ServerMode {
    fn to_resp(&self) -> RESP {
        match self {
            ServerMode::Master(MasterInfo {
                master_replid,
                master_repl_offset,
            }) => {
                let role = "role:master".to_string();
                let master_replid = format!("master_replid:{master_replid}");
                let master_repl_offset = format!("master_repl_offset:{master_repl_offset}");
                let string = [role, master_replid, master_repl_offset].join("\n");
                bulk!(string.into_bytes())
            }
            ServerMode::Slave(_) => bulk!(b"role:slave".to_vec()),
        }
    }
}
