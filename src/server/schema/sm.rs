use std::collections::HashMap;
use super::SchemaType;

pub static DEFAULT_RAFT_ID: u64 = hash_ident!(MORPHEUS_SCHEMA_RAFT_SM) as u64;

def_store_hash_map!(schema_types <u32, SchemaType>);
