use bifrost::rpc::*;
use bifrost::raft::RaftService;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use model::edge::{EdgeType};
use model::edge;
use chashmap::CHashMap;
use std::sync::Arc;
use neb::ram::schema::{Field, Schema};
use server::schema::sm::schema_types::client::SMClient;
use model::vertex::VERTEX_TEMPLATE;

mod sm;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SchemaType {
    Vertex,
    Edge(EdgeType)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SchemaError {
    ExecError(ExecError),
    SimpleEdgeShouldNotHaveSchema,
}

pub struct SchemaContainer {
    map: CHashMap<u32, SchemaType>,
    sm_client: Arc<SMClient>
}

pub fn head_fields(schema_type: SchemaType) -> Result<Vec<Field>, SchemaError> {
    Ok(match schema_type {
        SchemaType::Vertex => VERTEX_TEMPLATE.clone(),
        SchemaType::Edge(edge_type) => match edge_type {
            EdgeType::Direct => edge::direct::EDGE_TEMPLATE.clone(),
            EdgeType::Indirect => edge::indirect::EDGE_TEMPLATE.clone(),
            EdgeType::Hyper => edge::hyper::EDGE_TEMPLATE.clone(),
            EdgeType::Simple => return Err(SchemaError::SimpleEdgeShouldNotHaveSchema),
        }
    })
}

impl SchemaContainer {

    pub fn new_meta_service(raft_service: &Arc<RaftService>) {
        let mut container_sm = sm::schema_types::Map::new(sm::DEFAULT_RAFT_ID);
        container_sm.init_callback(raft_service);
        raft_service.register_state_machine(Box::new(container_sm));
    }

    pub fn new_client(raft_client: &Arc<RaftClient>) -> Result<Arc<SchemaContainer>, ExecError> {
        let sm_client = Arc::new(SMClient::new(sm::DEFAULT_RAFT_ID, &raft_client));
        let sm_entries = sm_client.entries()?.unwrap();
        let container = SchemaContainer {
            map: CHashMap::new(),
            sm_client: sm_client.clone()
        };
        let container_ref = Arc::new(container);
        let container_ref1 = container_ref.clone();
        let container_ref2 = container_ref.clone();
        for (schema_id, schema_type) in sm_entries {
            container_ref.map.insert(schema_id, schema_type);
        }
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

    pub fn new_schema(schema_type: SchemaType, schema: &mut Schema) -> Result<(), SchemaError> {
        let head_fields = head_fields(schema_type)?;
        // let schema_fields = ;
        Ok(())
    }
}