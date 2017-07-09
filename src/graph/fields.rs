use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, key_hash};

pub const INBOUND_KEY: &'static str = "_inbound";
pub const OUTBOUND_KEY: &'static str = "_outbound";
pub const INDIRECTED_KEY: &'static str = "_indirected";

lazy_static! {
    pub static ref INBOUND_NAME: String = String::from(INBOUND_KEY);
    pub static ref OUTBOUND_NAME: String = String::from(OUTBOUND_KEY);
    pub static ref INDIRECTED_NAME: String = String::from(INDIRECTED_KEY);
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&*OUTBOUND_NAME, TypeId::Id as u32, false, false, None),
            Field::new(&*INBOUND_NAME, TypeId::Id as u32, false, false, None),
            Field::new(&*INDIRECTED_NAME, TypeId::Id as u32, false, false, None)
        ];
    pub static ref INBOUND_KEY_ID: u64 = key_hash(&String::from(INBOUND_KEY));
    pub static ref OUTBOUND_KEY_ID: u64 = key_hash(&String::from(OUTBOUND_KEY));
    pub static ref INDIRECTED_KEY_ID: u64 = key_hash(&String::from(INDIRECTED_KEY));
}