use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::ram::cell::{Cell, WriteError};
use neb::client::{Client as NebClient};
use bifrost::rpc::RPCError;

use server::schema::{MorpheusSchema, SchemaType, SchemaContainer, SchemaError};
use graph::vertex::Vertex;

use std::sync::Arc;

pub mod vertex;
pub mod edge;

pub enum NewVertexError {
    SchemaNotFound,
    SchemaNotVertex,
    KeyFieldNotAvailable,
    DataNotMap,
    RPCError(RPCError),
    WriteError(WriteError),
}

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field::new(&String::from("next"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("list"), TypeId::Id as u32, false, true, None)
        ];
}

pub fn cell_to_vertex (cell: Cell) -> Vertex {
    Vertex {
        id: cell.header.id(),
        schema: cell.header.schema,
        data: cell.data
    }
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
        if edge_type == edge::EdgeType::Simple {
            return Err(SchemaError::SimpleEdgeShouldNotHaveSchema)
        }
        schema.schema_type = SchemaType::Edge(edge_type);
        self.schema.new_schema(schema)
    }
    pub fn new_vertex(&self, mut vertex: Vertex) -> Result<Vertex, NewVertexError> {
        let schema_id = vertex.schema;
        if let Some(stype) = self.schema.schema_type(schema_id) {
            if stype != SchemaType::Vertex {
                return Err(NewVertexError::SchemaNotVertex)
            }
        } else {
            return Err(NewVertexError::SchemaNotFound)
        }
        let neb_schema = match self.schema.get_neb_schema(schema_id) {
            Some(schema) => schema,
            None => return Err(NewVertexError::SchemaNotFound)
        };
        let mut data = {
            match vertex.data {
                Value::Map(mut map) => map,
                _ => return Err(NewVertexError::DataNotMap)
            }
        };
        let id = {match neb_schema.key_field {
                Some(ref keys) => {
                    let value = data.get_in_by_ids(keys.iter());
                    match value {
                        &Value::Null => return Err(NewVertexError::KeyFieldNotAvailable),
                        _ => Id::from_obj(&(schema_id, value))
                    }
                },
                None => {
                    if vertex.id.is_unit_id() {
                        Id::rand()
                    } else {
                        vertex.id
                    }
                }
            }
        };
        data.insert_key_id(*vertex::INBOUND_KEY_ID, Value::Id(Id::unit_id()));
        data.insert_key_id(*vertex::OUTBOUND_KEY_ID, Value::Id(Id::unit_id()));
        data.insert_key_id(*vertex::INDIRECTED_KEY_ID, Value::Id(Id::unit_id()));
        let mut cell = Cell::new(schema_id, &id, Value::Map(data));
        let header = match self.neb_client.write_cell(&cell) {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e))
        };
        cell.header = header;
        Ok(cell_to_vertex(cell))
    }
}