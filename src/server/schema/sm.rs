use std::collections::HashMap;
use super::SchemaType;

pub static DEFAULT_RAFT_PREFIX: &'static str = "MORPHEUS_SCHEMA_RAFT_SM";

def_store_hash_map!(schema_types <u32, SchemaType>);
