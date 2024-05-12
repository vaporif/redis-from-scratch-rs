use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
};

use anyhow::Context;
use tokio::net::TcpListener;
use tracing_appender::non_blocking::WorkerGuard;

fn init_tracing(file_name: String) -> WorkerGuard {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    env::set_var("RUST_LOG", "trace");
    let file_appender =
        tracing_appender::rolling::hourly("~/logs", format!("redis-{}.txt", file_name));
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    env::set_var("RUST_BACKTRACE", "full");
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .with(EnvFilter::from_default_env());

    #[cfg(feature = "debug")]
    {
        subscriber.with(console_subscriber::spawn()).init();
        println!("tokio console enabled");
    }

    #[cfg(not(feature = "debug"))]
    {
        subscriber.init();
    }

    guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use redis_from_scratch_rs::Parser;
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }

    let cli = redis_from_scratch_rs::Cli::parse();
    let replicaof = cli.replicaof()?;

    let log_file_name = if replicaof.is_none() {
        "master".to_string()
    } else {
        format!("replica-{}", cli.port).to_string()
    };

    let _write_guard = init_tracing(log_file_name);

    let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), cli.port);

    let listener = TcpListener::bind(socket)
        .await
        .context("listening on port")?;

    let (executor_messenger, join_handle) =
        redis_from_scratch_rs::spawn_actor_executor(replicaof, cli.port, cli.max_connections).await;

    tokio::spawn(async move {
        let mut tcp_server =
            redis_from_scratch_rs::TcpServer::new(listener, executor_messenger).await;
        tcp_server.start().await;
        tracing::trace!("tcp loop started");
    });

    if let Err(err) = join_handle.await.context("waiting on actor executor") {
        tracing::error!("fatal crash {:?}", err);

        Err(err)?;
    }

    Ok(())
}
