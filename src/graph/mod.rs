use neb::ram::schema::Field;
use neb::ram::types::TypeId;
use neb::client::{Client as NebClient};

use server::schema::{MorpheusSchema, SchemaType, SchemaContainer, SchemaError};

use std::sync::Arc;

pub mod vertex;
pub mod edge;

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field::new(&String::from("next"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("list"), TypeId::Id as u32, false, true, None)
        ];
}

pub struct Graph {
    schema: Arc<SchemaContainer>,
    neb_client: Arc<NebClient>
}

impl Graph {
    pub fn new_vertex_group(&self, schema: &mut MorpheusSchema) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Vertex;
        self.schema.new_schema(schema)
    }
    pub fn new_edge_group(&self, schema: &mut MorpheusSchema, edge_type: edge::EdgeType) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Edge(edge_type);
        self.schema.new_schema(schema)
    }
}