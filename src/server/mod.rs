use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc;
use bifrost::tcp::STANDALONE_ADDRESS_STRING;
use futures::prelude::*;
use futures::{future, Future};
use neb::client::{AsyncClient as NebClient, NebClientError};
use neb::server::{NebServer, ServerError, ServerOptions as NebServerOptions};
use std::sync::Arc;

use crate::graph::Graph;

pub mod general;
pub mod schema;
pub mod traversal;

#[derive(Debug)]
pub enum MorpheusServerError {
    ServerError(ServerError),
    ClientError(NebClientError),
    InitSchemaError(ExecError),
}

pub struct MorpheusServer {
    pub neb_server: Arc<NebServer>,
    pub neb_client: Arc<NebClient>,
    pub schema_container: Arc<schema::SchemaContainer>,
    pub graph: Arc<Graph>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MorphesOptions {
    pub server_addr: String,
    pub group_name: String,
    pub storage: NebServerOptions,
    pub meta_members: Vec<String>,
}

impl MorpheusServer {
    pub async fn new(options: MorphesOptions) -> Result<Arc<MorpheusServer>, MorpheusServerError> {
        let neb_opts = &options.storage;
        let group_name = &options.group_name;
        let neb_server = NebServer::new_from_opts(neb_opts, &options.server_addr, group_name).await;
        let neb_client = Arc::new(
            neb::client::AsyncClient::new(
                &neb_server.rpc,
                &neb_server.membership,
                &options.meta_members,
                group_name,
            )
            .await
            .unwrap(),
        );
        let schema_container = schema::SchemaContainer::new_client(
            group_name,
            &neb_client.raft_client,
            &neb_client,
            &neb_server.meta,
        )
        .await
        .map_err(MorpheusServerError::InitSchemaError)?;
        let graph = Arc::new(
            Graph::new(&schema_container, &neb_client)
                .map_err(MorpheusServerError::InitSchemaError)
                .await?,
        );
        Ok(Arc::new(MorpheusServer {
            neb_server,
            neb_client,
            schema_container,
            graph,
        }))
    }
}
