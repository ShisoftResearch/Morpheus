use std::sync::Arc;
use neb::client::{Client as NebClient, NebClientError};
use neb::server::{ServerOptions, NebServer, ServerError};

pub mod general;
pub mod schema;
pub mod traversal;

pub enum MorpheusServerErrors {
    ServerError(ServerError),
    ClientError(NebClientError)
}

pub struct MorpheusServer {
    neb_server: Arc<NebServer>,
    neb_client: Arc<NebClient>
}

impl MorpheusServer {
    pub fn new(opts: &ServerOptions)
        -> Result<Arc<MorpheusServer>, MorpheusServerErrors> {
        let neb_server = match NebServer::new(opts) {
            Ok(server) => server,
            Err(e) => return Err(MorpheusServerErrors::ServerError(e))
        };
        let neb_client = match NebClient::new(
            &neb_server.rpc, &opts.meta_members,
            &opts.group_name) {
            Ok(client) => Arc::new(client),
            Err(e) => return Err(MorpheusServerErrors::ClientError(e))
        };
        Ok(Arc::new(MorpheusServer {
            neb_server: neb_server,
            neb_client: neb_client,
        }))
    }
}