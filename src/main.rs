#![feature(proc_macro)]
#![feature(plugin)]
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

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    info!("Shisoft Morpheus is initializing...");
}
