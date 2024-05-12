use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpListener;

use crate::prelude::*;

pub struct TcpServer {
    listener: TcpListener,
    executor_messenger: super::ExecutorMessenger,
}

impl TcpServer {
    pub async fn new(listener: TcpListener, executor_messenger: super::ExecutorMessenger) -> Self {
        Self {
            listener,
            executor_messenger,
        }
    }

    pub async fn start(&mut self) {
        if let Err(error) = self.accept_connections_loop().await {
            let _ = self
                .executor_messenger
                .connection_sender
                .send(super::executor::ConnectionMessage::FatalError(error))
                .await;
        }
    }

    async fn accept_connections_loop(&mut self) -> Result<(), std::io::Error> {
        trace!("listener started at {:?}", &self.listener);
        loop {
            let (stream, socket_addr) = self.listener.accept().await?;
            let socket_ref = SockRef::from(&stream);
            let ka = TcpKeepalive::new().with_time(std::time::Duration::from_secs(30));
            socket_ref.set_tcp_keepalive(&ka).unwrap();
            tracing::info!("new connection incoming {:?}", socket_addr);
            let _ = self
                .executor_messenger
                .connection_sender
                .send(super::executor::ConnectionMessage::NewConnection((
                    stream,
                    socket_addr,
                )))
                .await;
        }
    }
}
