use crate::config;
use crate::server::{MorpheusServer, MorpheusServerError};
use futures::Future;
use neb::server::ServerOptions;
use std::sync::Arc;

mod graph;

pub fn start_server<'a>(port: u32, group: &'a str) -> impl Future<Output = Result<Arc<MorpheusServer>, MorpheusServerError>> {
    let replacement_address: String = format!("127.0.0.1:{}", port);
    let mut config = config::options_from_file("config/neb.yaml");
    config.meta_members = vec![replacement_address.clone()];
    config.server_addr = replacement_address.clone();
    config.group_name = format!("{}-{}", group, "test");
    MorpheusServer::new(config)
}

#[test]
pub fn server_startup() {
    start_server(4000, "bootstrap");
}
