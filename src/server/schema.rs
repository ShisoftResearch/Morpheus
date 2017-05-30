use bifrost::rpc::*;
use neb::ram::schema::Field;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(MORPHEUS_SCHEMA_SERVICE) as u64;

service! {
    rpc new_vertex_type(name: String, fields: Vec<Field>);
    rpc new_edge_type(name: String, fields: Vec<Field>);
}