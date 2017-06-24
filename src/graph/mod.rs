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

pub fn cell_to_vertex(cell: Cell) -> Vertex {
    Vertex {
        id: cell.header.id(),
        schema: cell.header.schema,
        data: cell.data
    }
}

pub fn vertex_to_cell(vertex: Vertex) -> Cell {
    Cell::new_with_id(vertex.schema, &vertex.id, vertex.data)
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
        data.insert_key_id(*vertex::INBOUND_KEY_ID, Value::Id(Id::unit_id()));
        data.insert_key_id(*vertex::OUTBOUND_KEY_ID, Value::Id(Id::unit_id()));
        data.insert_key_id(*vertex::INDIRECTED_KEY_ID, Value::Id(Id::unit_id()));
        let mut cell = match Cell::new(&neb_schema, Value::Map(data)) {
            Some(cell) => cell,
            None => return Err(NewVertexError::CannotGenerateCellByData)
        };
        let header = match self.neb_client.write_cell(&cell) {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e))
        };
        cell.header = header;
        Ok(cell_to_vertex(cell))
    }
    // TODO: Update edge list
    pub fn remove_vertex(&self, id: &Id) -> Result<(), TxnError> {
        self.neb_client.transaction(|txn| {
            match txn.read(id)? {
                Some(cell) => {
                    let vertex = cell_to_vertex(cell); // for remove it from neighbourhoods
                    txn.remove(id)
                },
                None => txn.abort()
            }
        })
    }
    pub fn remove_vertex_by_key<K>(&self, schema_id: u32, key: &K) -> Result<(), TxnError>
        where K: Serialize {
        let id = Cell::encode_cell_key(schema_id, key);
        self.remove_vertex(&id)
    }
    pub fn update_vertex<U>(&self, id: &Id, update: U) -> Result<(), TxnError>
        where U: Fn(Vertex) -> Option<Vertex> {
        let update_cell = |cell| {
            match update(cell_to_vertex(cell)) {
                Some(vertex) => Some(vertex_to_cell(vertex)),
                None => None
            }
        };
        self.neb_client.transaction(|txn|{
            let cell = txn.read(id)?;
            match cell {
                Some(cell) => {
                    match update_cell(cell) {
                        Some(cell) => txn.update(&cell),
                        None => txn.abort()
                    }
                },
                None => txn.abort()
            }
        })
    }
    pub fn update_vertex_by_key<K, U>(&self, schema_id: u32, key: &K, update: U)
        -> Result<(), TxnError>
        where K: Serialize, U: Fn(Vertex) -> Option<Vertex>{
        let id = Cell::encode_cell_key(schema_id, key);
        self.update_vertex(&id, update)
    }

}