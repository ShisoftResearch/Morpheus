use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::ram::cell::{Cell, WriteError};
use neb::client::{Client as NebClient};
use neb::client::transaction::TxnError;
use bifrost::rpc::RPCError;

use server::schema::{MorpheusSchema, SchemaType, SchemaContainer, SchemaError};
use graph::vertex::Vertex;

use std::sync::Arc;
use serde::Serialize;

pub mod vertex;
pub mod edge;

pub enum NewVertexError {
    SchemaNotFound,
    SchemaNotVertex,
    CannotGenerateCellByData,
    DataNotMap,
    RPCError(RPCError),
    WriteError(WriteError),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum CellType {
    Vertex,
    Edge(edge::EdgeType)
}

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field::new(&String::from("next"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("list"), TypeId::Id as u32, false, true, None)
        ];
}

fn vertex_to_cell_for_write(schemas: &Arc<SchemaContainer>, vertex: Vertex) -> Result<Cell, NewVertexError> {
    let schema_id = vertex.schema;
    if let Some(stype) = schemas.schema_type(schema_id) {
        if stype != SchemaType::Vertex {
            return Err(NewVertexError::SchemaNotVertex)
        }
    } else {
        return Err(NewVertexError::SchemaNotFound)
    }
    let neb_schema = match schemas.get_neb_schema(schema_id) {
        Some(schema) => schema,
        None => return Err(NewVertexError::SchemaNotFound)
    };
    let mut data = {
        match vertex.data {
            Value::Map(mut map) => map,
            _ => return Err(NewVertexError::DataNotMap)
        }
    };
    data.insert_key_id(*vertex::INBOUND_KEY_ID, Value::Id(Id::unit_id()));
    data.insert_key_id(*vertex::OUTBOUND_KEY_ID, Value::Id(Id::unit_id()));
    data.insert_key_id(*vertex::INDIRECTED_KEY_ID, Value::Id(Id::unit_id()));
    match Cell::new(&neb_schema, Value::Map(data)) {
        Some(cell) => Ok(cell),
        None => return Err(NewVertexError::CannotGenerateCellByData)
    }
}

pub struct Graph {
    schemas: Arc<SchemaContainer>,
    neb_client: Arc<NebClient>
}

impl Graph {
    pub fn new_vertex_group(&self, schema: &mut MorpheusSchema) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Vertex;
        self.schemas.new_schema(schema)
    }
    pub fn new_edge_group(&self, schema: &mut MorpheusSchema, edge_type: edge::EdgeType) -> Result<(), SchemaError> {
        if edge_type == edge::EdgeType::Simple {
            return Err(SchemaError::SimpleEdgeShouldNotHaveSchema)
        }
        schema.schema_type = SchemaType::Edge(edge_type);
        self.schemas.new_schema(schema)
    }
    pub fn new_vertex(&self, vertex: Vertex) -> Result<Vertex, NewVertexError> {
        let mut cell = vertex_to_cell_for_write(&self.schemas, vertex)?;
        let header = match self.neb_client.write_cell(&cell) {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e))
        };
        cell.header = header;
        Ok(vertex::cell_to_vertex(cell))
    }
    // TODO: Update edge list
    pub fn remove_vertex(&self, id: &Id) -> Result<(), TxnError> {
        self.neb_client.transaction(|mut txn| {
            vertex::txn_remove(&mut txn, id)
        })
    }
    pub fn remove_vertex_by_key<K>(&self, schema_id: u32, key: &K) -> Result<(), TxnError>
        where K: Serialize {
        let id = Cell::encode_cell_key(schema_id, key);
        self.remove_vertex(&id)
    }
    pub fn update_vertex<U>(&self, id: &Id, update: U) -> Result<(), TxnError>
        where U: Fn(Vertex) -> Option<Vertex> {
        self.neb_client.transaction(|txn|{
            vertex::txn_update(txn, id, &update)
        })
    }
    pub fn update_vertex_by_key<K, U>(&self, schema_id: u32, key: &K, update: U)
        -> Result<(), TxnError>
        where K: Serialize, U: Fn(Vertex) -> Option<Vertex>{
        let id = Cell::encode_cell_key(schema_id, key);
        self.update_vertex(&id, update)
    }

}