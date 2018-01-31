use bifrost::rpc;
use std::sync::Arc;
use bifrost::raft::state_machine::master::ExecError;
use neb::client::{AsyncClient as NebClient, NebClientError};
use neb::server::{ServerOptions as NebServerOptions, NebServer, ServerError};
use hivemind::server::{ServerOptions as HMServerOptions};
use bifrost::tcp::{STANDALONE_ADDRESS_STRING};

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
    pub fn new(
        neb_opts: &NebServerOptions
    ) -> Result<Arc<MorpheusServer>, MorpheusServerError> {
        let server_addr = if neb_opts.standalone {&STANDALONE_ADDRESS_STRING} else {&neb_opts.address};
        let rpc_server = rpc::Server::new(server_addr);
        rpc::Server::listen_and_resume(&rpc_server);
        if !neb_opts.is_meta && neb_opts.standalone {
            return Err(MorpheusServerError::ServerError(ServerError::StandaloneMustAlsoBeMetaServer))
        }

        let neb_server = NebServer::new(
            neb_opts, server_addr, &rpc_server
        ).map_err(MorpheusServerError::ServerError)?;
        let neb_client = Arc::new(NebClient::new(
            &neb_server.rpc, &neb_opts.meta_members,
            &neb_opts.group_name).map_err(MorpheusServerError::ClientError)?);
        if neb_opts.is_meta {
            if let &Some(ref raft_service) = &neb_server.raft_service {
                schema::SchemaContainer::new_meta_service(&neb_opts.group_name, raft_service);
            } else {
                panic!("raft service should be ready for meta server");
            }
        }
        let schema_container = schema::SchemaContainer::new_client(
            &neb_opts.group_name, &neb_client.raft_client, &neb_client, &neb_server.meta
        ).map_err(MorpheusServerError::InitSchemaError)?;
        let graph = Arc::new(Graph::new(&schema_container, &neb_client)
            .map_err(MorpheusServerError::InitSchemaError)?);
        Ok(Arc::new(MorpheusServer {
            neb_server,
            neb_client,
            schema_container,
            graph
        }))
    }
}