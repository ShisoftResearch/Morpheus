use neb::server::ServerOptions;
use server::MorpheusServer;
use config;
use std::sync::Arc;

mod graph;

pub fn start_server<'a>(port: u32, group: &'a str) -> Arc<MorpheusServer> {
    let replacement_address: String = format!("127.0.0.1:{}", port);
    let mut neb_config: ServerOptions = config::neb::options_from_file("config/neb.yaml");
    neb_config.meta_members = vec![replacement_address.clone()];
    neb_config.address = replacement_address.clone();
    neb_config.group_name = format!("{}-{}", group, "test");
    MorpheusServer::new(&neb_config).unwrap()
}

#[test]
pub fn server_startup() {
    start_server(4000, "bootstrap");
}