use std::sync::Arc;
use bifrost::raft::state_machine::master::ExecError;
use neb::client::{Client as NebClient, NebClientError};
use neb::server::{ServerOptions, NebServer, ServerError};

use graph::Graph;

pub mod general;
pub mod schema;
pub mod traversal;

#[derive(Debug)]
pub enum MorpheusServerError {
    ServerError(ServerError),
    ClientError(NebClientError),
    InitSchemaError(ExecError)
}

pub struct MorpheusServer {
    pub neb_server: Arc<NebServer>,
    pub neb_client: Arc<NebClient>,
    pub schema_container: Arc<schema::SchemaContainer>,
    pub graph: Arc<Graph>
}

impl MorpheusServer {
    pub fn new(opts: &ServerOptions)
               -> Result<Arc<MorpheusServer>, MorpheusServerError> {
        let neb_server = NebServer::new(opts).map_err(MorpheusServerError::ServerError)?;
        let neb_client = Arc::new(NebClient::new(
            &neb_server.rpc, &opts.meta_members,
            &opts.group_name).map_err(MorpheusServerError::ClientError)?);
        if opts.is_meta {
            if let &Some(ref raft_service) = &neb_server.raft_service {
                schema::SchemaContainer::new_meta_service(&opts.group_name, raft_service);
            } else {
                panic!("raft service should be ready for meta server");
            }
        }
        let schema_container = schema::SchemaContainer::new_client(
            &opts.group_name, &neb_client.raft_client, &neb_client, &neb_server.meta
        ).map_err(MorpheusServerError::InitSchemaError)?;
        let graph = Arc::new(Graph::new(&schema_container, &neb_client)
            .map_err(MorpheusServerError::InitSchemaError)?);
        Ok(Arc::new(MorpheusServer {
            neb_server: neb_server,
            neb_client: neb_client,
            schema_container: schema_container,
            graph: graph
        }))
    }
}