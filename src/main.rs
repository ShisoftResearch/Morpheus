#![feature(proc_macro)]
#![feature(plugin)]
#![feature(conservative_impl_trait)]
#![plugin(bifrost_plugins)]

extern crate neb;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
#[macro_use]
extern crate bifrost_hasher;
extern crate futures;
extern crate parking_lot;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate chashmap;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate yaml_rust;
extern crate serde_yaml;

mod graph;
mod server;
mod utils;
mod config;

use neb::server::*;
use std::thread;

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    info!("Shisoft Morpheus is initializing...");
    let neb_config = config::neb::options_from_file("config/neb.yaml");
    let morpheus_server = server::MorpheusServer::new(&neb_config).unwrap();

    thread::park();
}
