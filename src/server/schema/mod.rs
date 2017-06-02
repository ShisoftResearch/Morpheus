use bifrost::rpc::*;
use neb::ram::schema::Field;
use model::edge::{EdgeType};
use chashmap::CHashMap;
use std::sync::Arc;

pub static DEFAULT_RAFT_ID: u64 = hash_ident!(MORPHEUS_SCHEMA_RAFT_SM) as u64;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SchemaType {
    Vertex,
    Edge(EdgeType)
}

pub struct SchemaContainer {
    map: CHashMap<u32, SchemaType>
}

impl SchemaContainer {
    pub fn new() -> Arc<SchemaContainer> {
        let container = SchemaContainer {
            map: CHashMap::new()
        };
        Arc::new(container)
    }
}