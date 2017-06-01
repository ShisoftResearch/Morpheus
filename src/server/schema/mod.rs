use bifrost::rpc::*;
use neb::ram::schema::Field;
use model::edge::{EdgeType};

pub static DEFAULT_RAFT_ID: u64 = hash_ident!(MORPHEUS_SCHEMA_RAFT_SM) as u64;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SchemaType {
    Vertex,
    Edge(EdgeType)
}

