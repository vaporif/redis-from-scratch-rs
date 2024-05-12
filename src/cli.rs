use crate::{prelude::*, MasterAddr};
pub use clap::Parser;
use clap::{arg, command};

#[derive(Parser, Debug)]
#[command(author = "Dmytro Onypko", name = "Redis Sample Server")]
pub struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,
    #[arg(short, long, default_value_t = 10_000)]
    pub max_connections: usize,
    #[arg(short, long, value_names = ["MASTER_HOST", "MASTER_PORT"], number_of_values = 2, value_delimiter = ' ')]
    replicaof: Option<Vec<String>>,
}

impl Cli {
    pub fn replicaof(&self) -> anyhow::Result<Option<MasterAddr>> {
        let Some(values) = self.replicaof.clone() else {
            return Ok(None);
        };

        let [host, port] = values.as_slice() else {
            unreachable!()
        };

        let port: u16 = port.parse().context("could not parse replicaof port")?;

        Ok(Some((host.clone(), port)))
    }
}
