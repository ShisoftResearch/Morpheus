#![feature(proc_macro)]
#![feature(plugin)]
#![feature(conservative_impl_trait)]

extern crate neb;
#[macro_use]
extern crate lazy_static;
extern crate bifrost;
extern crate bifrost_hasher;
#[macro_use]
extern crate bifrost_plugins;
extern crate parking_lot;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate log4rs;
extern crate serde_yaml;
extern crate yaml_rust;

use futures::Future;

mod config;
mod graph;
mod query;
mod server;
#[cfg(test)]
mod tests;
mod utils;

use std::thread;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    info!("Shisoft Morpheus is initializing...");
    query::init().unwrap();
    let neb_config = config::neb::options_from_file("config/neb.yaml");
    server::MorpheusServer::new(neb_config).await.unwrap();
}
