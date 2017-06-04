use bifrost::rpc::*;
use bifrost::raft::RaftService;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use model::edge::{EdgeType};
use model::edge;
use chashmap::CHashMap;
use std::sync::Arc;
use neb::ram::schema::{Field, Schema};
use neb::client::{Client as NebClient};
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
    NewNebSchemaExecError(ExecError),
    NewMorpheusSchemaExecError(ExecError),
    SimpleEdgeShouldNotHaveSchema,
}

pub struct SchemaContainer {
    map: CHashMap<u32, SchemaType>,
    sm_client: Arc<SMClient>,
    neb_client: Arc<NebClient>
}

pub struct MorpheusSchema {
    pub id: u32,
    pub name: String,
    pub key_field: Option<Vec<String>>,
    pub fields: Vec<Field>
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

    pub fn new_client(raft_client: &Arc<RaftClient>, neb_client: &Arc<NebClient>) -> Result<Arc<SchemaContainer>, ExecError> {
        let sm_client = Arc::new(SMClient::new(sm::DEFAULT_RAFT_ID, &raft_client));
        let sm_entries = sm_client.entries()?.unwrap();
        let container = SchemaContainer {
            map: CHashMap::new(),
            sm_client: sm_client.clone(),
            neb_client: neb_client.clone()
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

    pub fn new_schema(&self, schema_type: SchemaType, schema: &mut MorpheusSchema) -> Result<(), SchemaError> {
        let mut schema_fields = head_fields(schema_type)?;
        schema_fields.append(&mut schema.fields);
        let mut neb_schema = Schema::new(
            schema.name.clone(),
            schema.key_field.clone(),
            Field::new(&String::from("*"), 0, false, false, Some(schema_fields)));
        match self.neb_client.new_schema(&mut neb_schema) {
            Ok(()) => {},
            Err(e) => return Err(SchemaError::NewNebSchemaExecError(e))
        };
        let schema_id = neb_schema.id;
        match self.sm_client.insert(&schema_id, &schema_type) {
            Ok(_) => {},
            Err(e) => return Err(SchemaError::NewMorpheusSchemaExecError(e))
        }
        schema.id = schema_id;
        Ok(())
    }
}