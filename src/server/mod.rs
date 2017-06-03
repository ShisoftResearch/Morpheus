use std::sync::Arc;
use bifrost::raft::state_machine::master::ExecError;
use neb::client::{Client as NebClient, NebClientError};
use neb::server::{ServerOptions, NebServer, ServerError};

pub mod general;
pub mod schema;
pub mod traversal;

pub enum MorpheusServerError {
    ServerError(ServerError),
    ClientError(NebClientError),
    InitSchemaError(ExecError)
}

pub struct MorpheusServer {
    neb_server: Arc<NebServer>,
    neb_client: Arc<NebClient>
}

impl MorpheusServer {
    pub fn new(opts: &ServerOptions)
        -> Result<Arc<MorpheusServer>, MorpheusServerError> {
        let neb_server = match NebServer::new(opts) {
            Ok(server) => server,
            Err(e) => return Err(MorpheusServerError::ServerError(e))
        };
        let neb_client = match NebClient::new(
            &neb_server.rpc, &opts.meta_members,
            &opts.group_name) {
            Ok(client) => Arc::new(client),
            Err(e) => return Err(MorpheusServerError::ClientError(e))
        };
        if opts.is_meta {
            if let &Some(ref raft_service) = &neb_server.raft_service {
                schema::SchemaContainer::new_meta_service(raft_service);
            } else {
                panic!("raft service should be ready for meta server");
            }
        }
        let schema_container = match schema::SchemaContainer::new_client(&neb_client.raft_client) {
            Ok(container) => container,
            Err(e) => return Err(MorpheusServerError::InitSchemaError(e))
        };
        Ok(Arc::new(MorpheusServer {
            neb_server: neb_server,
            neb_client: neb_client,
        }))
    }
}