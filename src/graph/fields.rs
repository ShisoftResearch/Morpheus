use dovahkiin::types::Type;
use neb::ram::schema::Field;
use neb::ram::types::{Id, key_hash};

pub const INBOUND_KEY: &'static str = "_inbound";
pub const OUTBOUND_KEY: &'static str = "_outbound";
pub const UNDIRECTED_KEY: &'static str = "_undirected";

lazy_static! {
    pub static ref INBOUND_NAME: String = String::from(INBOUND_KEY);
    pub static ref OUTBOUND_NAME: String = String::from(OUTBOUND_KEY);
    pub static ref UNDIRECTED_NAME: String = String::from(UNDIRECTED_KEY);
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&*OUTBOUND_NAME, Type::Id, false, false, None, vec![]),
            Field::new(&*INBOUND_NAME, Type::Id, false, false, None, vec![]),
            Field::new(&*UNDIRECTED_NAME, Type::Id, false, false, None, vec![]),
        ];
    pub static ref INBOUND_KEY_ID: u64 = key_hash(&*INBOUND_NAME);
    pub static ref OUTBOUND_KEY_ID: u64 = key_hash(&*OUTBOUND_NAME);
    pub static ref UNDIRECTED_KEY_ID: u64 = key_hash(&*UNDIRECTED_NAME);
}