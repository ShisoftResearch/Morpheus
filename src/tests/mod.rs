use crate::config;
use crate::server::{MorpheusServer, MorpheusServerError};
use futures::Future;
use std::sync::Arc;

mod graph;

pub fn start_server<'a>(
    port: u32,
    group: &'a str,
) -> impl Future<Output = Result<Arc<MorpheusServer>, MorpheusServerError>> {
    let replacement_address: String = format!("127.0.0.1:{}", port);
    let mut config = config::options_from_file("config/test_server.yaml");
    config.meta_members = vec![replacement_address.clone()];
    config.server_addr = replacement_address.clone();
    config.group_name = format!("{}-{}", group, "test");
    MorpheusServer::new(config)
}

#[tokio::test]
pub async fn server_startup() {
    let _ = env_logger::try_init();
    start_server(4000, "bootstrap").await.unwrap();
}
