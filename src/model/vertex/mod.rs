use neb::ram::schema::Field;
use neb::ram::types::TypeId;
use neb::ram::cell::Cell;
use neb::client::{Client as NebClient};
use server::schema::{MorpheusSchema, SchemaType, SchemaContainer};
use std::sync::Arc;

lazy_static! {
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&String::from("_inbound"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("_outbound"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("_indirected"), TypeId::Id as u32, true, false, None)
        ];
}

pub struct VertexFactory {
    schema: Arc<SchemaContainer>,
    neb_client: Arc<NebClient>
}