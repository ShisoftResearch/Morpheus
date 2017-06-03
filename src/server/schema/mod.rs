use bifrost::rpc::*;
use bifrost::raft::RaftService;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use model::edge::{EdgeType};
use chashmap::CHashMap;
use std::sync::Arc;
use neb::ram::schema::Field;
use server::schema::sm::schema_types::client::SMClient;

mod sm;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SchemaType {
    Vertex,
    Edge(EdgeType)
}

pub struct SchemaContainer {
    map: CHashMap<u32, SchemaType>
}

impl SchemaContainer {

    pub fn new_meta_service(raft_service: &Arc<RaftService>) {
        let mut container_sm = sm::schema_types::Map::new(sm::DEFAULT_RAFT_ID);
        container_sm.init_callback(raft_service);
        raft_service.register_state_machine(Box::new(container_sm));
    }

    pub fn new_client(raft_client: &Arc<RaftClient>) -> Result<Arc<SchemaContainer>, ExecError> {
        let container = SchemaContainer {
            map: CHashMap::new()
        };
        let container_ref = Arc::new(container);
        let container_ref1 = container_ref.clone();
        let container_ref2 = container_ref.clone();
        let sm_client = SMClient::new(sm::DEFAULT_RAFT_ID, &raft_client);
        sm_client.on_inserted(move |res| {
            if let Ok((id, schema_type)) = res {
                container_ref1.map.insert(id, schema_type);
            }
        })?;
        sm_client.on_removed(move |res| {
            if let Ok((id, schema_type)) = res {
                container_ref2.map.remove(&id);
            }
        })?;
        return Ok(container_ref);
    }
}