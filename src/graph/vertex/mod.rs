use neb::ram::schema::Field;
use neb::ram::cell::Cell;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};

pub const INBOUND_KEY: &'static str = "_inbound";
pub const OUTBOUND_KEY: &'static str = "_outbound";
pub const INDIRECTED_KEY: &'static str = "_indirected";

lazy_static! {
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&String::from(INBOUND_KEY), TypeId::Id as u32, true, false, None),
            Field::new(&String::from(OUTBOUND_KEY), TypeId::Id as u32, true, false, None),
            Field::new(&String::from(INDIRECTED_KEY), TypeId::Id as u32, true, false, None)
        ];
    pub static ref INBOUND_KEY_ID: u64 = key_hash(&String::from(INBOUND_KEY));
    pub static ref OUTBOUND_KEY_ID: u64 = key_hash(&String::from(OUTBOUND_KEY));
    pub static ref INDIRECTED_KEY_ID: u64 = key_hash(&String::from(INDIRECTED_KEY));
}

pub struct Vertex {
    pub id: Id,
    pub schema: u32,
    pub data: Value,
}

impl Vertex {
    pub fn new(schema: u32, data: Map) -> Vertex {
        Vertex {
            id: Id::unit_id(),
            schema: schema,
            data: Value::Map(data)
        }
    }
}