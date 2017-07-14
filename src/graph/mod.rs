use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::ram::cell::{Cell, WriteError, ReadError};
use neb::client::{Client as NebClient};
use neb::client::transaction::{Transaction, TxnError};
use bifrost::rpc::RPCError;

use server::schema::{MorpheusSchema, SchemaType, SchemaContainer, SchemaError};
use graph::vertex::Vertex;
use graph::edge::bilateral::BilateralEdge;

use std::sync::Arc;
use serde::Serialize;

pub mod vertex;
pub mod edge;
pub mod fields;
mod id_list;
#[cfg(test)]
mod test;

pub enum NewVertexError {
    SchemaNotFound,
    SchemaNotVertex,
    CannotGenerateCellByData,
    DataNotMap,
    RPCError(RPCError),
    WriteError(WriteError)
}

pub enum ReadVertexError {
    RPCError(RPCError),
    ReadError(ReadError),
}

pub enum LinkVerticesError {
    EdgeSchemaNotFound,
    SchemaNotEdge,
    BodyRequired,
    BodyShouldNotExisted,
    EdgeError(edge::EdgeError),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum CellType {
    Vertex,
    Edge(edge::EdgeType)
}

fn vertex_to_cell_for_write(schemas: &Arc<SchemaContainer>, vertex: Vertex) -> Result<Cell, NewVertexError> {
    let schema_id = vertex.schema();
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
        match vertex.cell.data {
            Value::Map(map) => map,
            _ => return Err(NewVertexError::DataNotMap)
        }
    };
    data.insert_key_id(*fields::INBOUND_KEY_ID, Value::Id(Id::unit_id()));
    data.insert_key_id(*fields::OUTBOUND_KEY_ID, Value::Id(Id::unit_id()));
    data.insert_key_id(*fields::UNDIRECTED_KEY_ID, Value::Id(Id::unit_id()));
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
    pub fn new(schemas: &Arc<SchemaContainer>, neb_client: &Arc<NebClient>) -> Graph {
        Graph {
            schemas: schemas.clone(),
            neb_client: neb_client.clone()
        }
    }
    pub fn new_vertex_group(&self, schema: &mut MorpheusSchema) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Vertex;
        self.schemas.new_schema(schema)
    }
    pub fn new_edge_group(&self, schema: &mut MorpheusSchema, edge_attrs: edge::EdgeAttributes) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Edge(edge_attrs);
        self.schemas.new_schema(schema)
    }
    pub fn new_vertex(&self, schema_id: u32, data: Map) -> Result<Vertex, NewVertexError> {
        let vertex = Vertex::new(schema_id, data);
        let mut cell = vertex_to_cell_for_write(&self.schemas, vertex)?;
        let header = match self.neb_client.write_cell(&cell) {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e))
        };
        cell.header = header;
        Ok(vertex::cell_to_vertex(cell))
    }
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

    pub fn read_vertex(&self, id: &Id) -> Result<Option<Vertex>, ReadVertexError> {
        match self.neb_client.read_cell(id) {
            Err(e) => Err(ReadVertexError::RPCError(e)),
            Ok(Err(ReadError::CellDoesNotExisted)) => Ok(None),
            Ok(Err(e)) => Err(ReadVertexError::ReadError(e)),
            Ok(Ok(cell)) => Ok(Some(vertex::cell_to_vertex(cell)))
        }
    }

    pub fn get_vertex<K>(&self, schema_id: u32, key: &K) -> Result<Option<Vertex>, ReadVertexError>
        where K: Serialize {
        let id = Cell::encode_cell_key(schema_id, key);
        self.read_vertex(&id)
    }

    pub fn graph_transaction<TFN>(&self, func: TFN) -> Result<(), TxnError>
        where TFN: Fn(&mut GraphTransaction) -> Result<(), TxnError>
    {
        let wrapper = |neb_txn: &mut Transaction| {
            func(&mut GraphTransaction {
                neb_txn: neb_txn,
                schemas: self.schemas.clone()
            })
        };
        self.neb_client.transaction(wrapper)
    }
}

pub struct GraphTransaction<'a> {
    pub neb_txn: & 'a mut Transaction,
    schemas: Arc<SchemaContainer>
}

impl <'a>GraphTransaction<'a> {
    pub fn new_vertex(&mut self, schema_id: u32, data: Map)
        -> Result<Result<Vertex, NewVertexError>, TxnError> {
        let vertex = Vertex::new(schema_id, data);
        let mut cell = match vertex_to_cell_for_write(&self.schemas, vertex) {
            Ok(cell) => cell, Err(e) => return Ok(Err(e))
        };
        self.neb_txn.write(&cell)?;
        Ok(Ok(vertex::cell_to_vertex(cell)))
    }
    // TODO: Update edge list
    pub fn remove_vertex(&mut self, id: &Id) -> Result<(), TxnError> {
        vertex::txn_remove(self.neb_txn, id)
    }
    pub fn remove_vertex_by_key<K>(&mut self, schema_id: u32, key: &K) -> Result<(), TxnError>
        where K: Serialize {
        let id = Cell::encode_cell_key(schema_id, key);
        self.remove_vertex(&id)
    }

    pub fn link(&mut self, schema_id: u32, from_id: &Id, to_id: &Id, body: Option<Map>)
        -> Result<Result<edge::Edge, LinkVerticesError>, TxnError> {
        let edge_attr = match self.schemas.schema_type(schema_id) {
            Some(SchemaType::Edge(ea)) => ea,
            Some(_) => return Ok(Err(LinkVerticesError::SchemaNotEdge)),
            None => return Ok(Err(LinkVerticesError::EdgeSchemaNotFound))
        };
        match edge_attr.edge_type {
            edge::EdgeType::Directed =>
                Ok(edge::directed::DirectedEdge::link(from_id, to_id, body, &mut self.neb_txn, schema_id, &self.schemas)?
                    .map_err(LinkVerticesError::EdgeError).map(edge::Edge::Directed)),

            edge::EdgeType::Undirected =>
                Ok(edge::undirectd::UndirectedEdge::link(from_id, to_id, body, &mut self.neb_txn, schema_id, &self.schemas)?
                    .map_err(LinkVerticesError::EdgeError).map(edge::Edge::Undirected))
        }
    }
    pub fn update_vertex<U>(&mut self, id: &Id, update: U) -> Result<(), TxnError>
        where U: Fn(Vertex) -> Option<Vertex> {
        vertex::txn_update(self.neb_txn, id, &update)
    }
    pub fn update_vertex_by_key<K, U>(&mut self, schema_id: u32, key: &K, update: U)
        -> Result<(), TxnError>
        where K: Serialize, U: Fn(Vertex) -> Option<Vertex>{
        let id = Cell::encode_cell_key(schema_id, key);
        self.update_vertex(&id, update)
    }

    pub fn read_vertex(&mut self, id: &Id) -> Result<Option<Vertex>, TxnError> {
        self.neb_txn.read(id).map(|c| c.map(vertex::cell_to_vertex))
    }

    pub fn get_vertex<K>(&mut self, schema_id: u32, key: &K) -> Result<Option<Vertex>, TxnError>
        where K: Serialize {
        let id = Cell::encode_cell_key(schema_id, key);
        self.read_vertex(&id)
    }
}