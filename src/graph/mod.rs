use neb::ram::schema::{Field, Schema};
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::ram::cell::{Cell, WriteError, ReadError};
use neb::client::{Client as NebClient};
use neb::client::transaction::{Transaction, TxnError};
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::RPCError;

use server::schema::{MorpheusSchema, SchemaType, SchemaContainer, SchemaError, ToSchemaId};
use graph::vertex::{Vertex, ToVertexId};
use graph::edge::bilateral::BilateralEdge;

use std::sync::Arc;
use serde::Serialize;

pub mod vertex;
pub mod edge;
pub mod fields;
mod id_list;

#[derive(Debug)]
pub enum NewVertexError {
    SchemaNotFound,
    SchemaNotVertex(SchemaType),
    CannotGenerateCellByData,
    DataNotMap,
    RPCError(RPCError),
    WriteError(WriteError)
}

#[derive(Debug)]
pub enum ReadVertexError {
    RPCError(RPCError),
    ReadError(ReadError),
}

#[derive(Debug)]
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

#[derive(Clone, Copy)]
pub enum EdgeDirection {
    Inbound,
    Outbound,
    Undirected
}

impl EdgeDirection {
    pub fn as_field(&self) -> u64 {
        match self {
            &EdgeDirection::Inbound => *fields::INBOUND_KEY_ID,
            &EdgeDirection::Outbound => *fields::OUTBOUND_KEY_ID,
            &EdgeDirection::Undirected => *fields::UNDIRECTED_KEY_ID,
        }
    }
}

fn vertex_to_cell_for_write(schemas: &Arc<SchemaContainer>, vertex: Vertex) -> Result<Cell, NewVertexError> {
    let schema_id = vertex.schema();
    if let Some(stype) = schemas.schema_type(schema_id) {
        if stype != SchemaType::Vertex {
            return Err(NewVertexError::SchemaNotVertex(stype))
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
    pub fn new(schemas: &Arc<SchemaContainer>, neb_client: &Arc<NebClient>) -> Result<Graph, ExecError> {
        Graph::check_base_schemas(schemas)?;
        Ok(Graph {
            schemas: schemas.clone(),
            neb_client: neb_client.clone()
        })
    }
    fn check_base_schema<'a>(schemas: &Arc<SchemaContainer>, schema_id: u32, schema_name: & 'a str, fields: &Field) -> Result<(), ExecError> {
        match schemas.get_neb_schema(schema_id) {
            None => {
                schemas.neb_client.new_schema_with_id(
                    &Schema::new_with_id(
                        schema_id, schema_name, None, fields.clone(), false
                    )
                )?;
            },
            _ => {}
        }
        Ok(())
    }
    fn check_base_schemas(schemas: &Arc<SchemaContainer>) -> Result<(), ExecError> {
        Graph::check_base_schema(schemas, id_list::ID_LIST_SCHEMA_ID, "_NEB_ID_LIST", &*id_list::ID_LINKED_LIST)?;
        Graph::check_base_schema(schemas, id_list::TYPE_LIST_SCHEMA_ID, "_NEB_TYPE_ID_LIST", &*id_list::ID_TYPE_LIST)?;
        Ok(())
    }
    pub fn new_vertex_group(&self, schema: &mut MorpheusSchema) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Vertex;
        self.schemas.new_schema(schema)
    }
    pub fn new_edge_group(&self, schema: &mut MorpheusSchema, edge_attrs: edge::EdgeAttributes) -> Result<(), SchemaError> {
        schema.schema_type = SchemaType::Edge(edge_attrs);
        self.schemas.new_schema(schema)?;
        Ok(())
    }
    pub fn new_vertex<S>(&self, schema: S, data: Map) -> Result<Vertex, NewVertexError>
        where S: ToSchemaId {
        let vertex = Vertex::new(schema.to_id(&self.schemas), data);
        let mut cell = vertex_to_cell_for_write(&self.schemas, vertex)?;
        let header = match self.neb_client.write_cell(&cell) {
            Ok(Ok(header)) => header,
            Ok(Err(e)) => return Err(NewVertexError::WriteError(e)),
            Err(e) => return Err(NewVertexError::RPCError(e))
        };
        cell.header = header;
        Ok(vertex::cell_to_vertex(cell))
    }
    pub fn remove_vertex<V>(&self, vertex: V)
        -> Result<(), TxnError> where V: ToVertexId {
        let id = vertex.to_id();
        self.graph_transaction(|txn| txn.remove_vertex(id)?.map_err(|_| TxnError::Aborted))
    }
    pub fn remove_vertex_by_key<K, S>(&self, schema: S, key: &K) -> Result<(), TxnError>
        where K: Serialize, S: ToSchemaId {
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), key);
        self.remove_vertex(&id)
    }
    pub fn update_vertex<V, U>(&self, vertex: V, update: U) -> Result<(), TxnError>
        where V: ToVertexId, U: Fn(Vertex) -> Option<Vertex> {
        let id = vertex.to_id();
        self.neb_client.transaction(|txn|{
            vertex::txn_update(txn, id, &update)
        })
    }
    pub fn update_vertex_by_key<K, U, S>(&self, schema: S, key: &K, update: U)
        -> Result<(), TxnError>
        where K: Serialize, S: ToSchemaId, U: Fn(Vertex) -> Option<Vertex>{
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), key);
        self.update_vertex(&id, update)
    }

    pub fn vertex_by<V>(&self, vertex: V)
                        -> Result<Option<Vertex>, ReadVertexError> where V: ToVertexId {
        match self.neb_client.read_cell(&vertex.to_id()) {
            Err(e) => Err(ReadVertexError::RPCError(e)),
            Ok(Err(ReadError::CellDoesNotExisted)) => Ok(None),
            Ok(Err(e)) => Err(ReadVertexError::ReadError(e)),
            Ok(Ok(cell)) => Ok(Some(vertex::cell_to_vertex(cell)))
        }
    }

    pub fn vertex_by_key<K, S>(&self, schema: S, key: K) -> Result<Option<Vertex>, ReadVertexError>
        where K: Serialize, S: ToSchemaId {
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), &key);
        self.vertex_by(&id)
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
    pub fn new_vertex<S>(&mut self, schema: S, data: Map)
        -> Result<Result<Vertex, NewVertexError>, TxnError>
        where S: ToSchemaId{
        let vertex = Vertex::new(schema.to_id(&self.schemas), data);
        let mut cell = match vertex_to_cell_for_write(&self.schemas, vertex) {
            Ok(cell) => cell, Err(e) => return Ok(Err(e))
        };
        self.neb_txn.write(&cell)?;
        Ok(Ok(vertex::cell_to_vertex(cell)))
    }
    pub fn remove_vertex<V>(&mut self, vertex: V)
        -> Result<Result<(), vertex::RemoveError>, TxnError> where V: ToVertexId{
        vertex::txn_remove(self.neb_txn, &self.schemas, vertex)
    }
    pub fn remove_vertex_by_key<K, S>(&mut self, schema: S, key: &K)
        -> Result<Result<(), vertex::RemoveError>, TxnError>
        where K: Serialize, S: ToSchemaId {
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), key);
        self.remove_vertex(&id)
    }

    pub fn link<V, S>(&mut self, schema: S, from: V, to: V, body: Option<Map>)
        -> Result<Result<edge::Edge, LinkVerticesError>, TxnError>
        where V: ToVertexId, S: ToSchemaId {
        let from_id = &from.to_id();
        let to_id = &to.to_id();
        let schema_id = schema.to_id(&self.schemas);
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
    pub fn update_vertex<V, U>(&mut self, vertex: V, update: U) -> Result<(), TxnError>
        where V: ToVertexId, U: Fn(Vertex) -> Option<Vertex> {
        vertex::txn_update(self.neb_txn, vertex, &update)
    }
    pub fn update_vertex_by_key<K, U, S>(&mut self, schema: S, key: &K, update: U)
        -> Result<(), TxnError>
        where K: Serialize, S: ToSchemaId, U: Fn(Vertex) -> Option<Vertex>{
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), key);
        self.update_vertex(&id, update)
    }

    pub fn read_vertex<V>(&mut self, vertex: V)
        -> Result<Option<Vertex>, TxnError> where V: ToVertexId {
        self.neb_txn.read(&vertex.to_id()).map(|c| c.map(vertex::cell_to_vertex))
    }

    pub fn get_vertex<K, S>(&mut self, schema: u32, key: &K) -> Result<Option<Vertex>, TxnError>
        where K: Serialize, S: ToSchemaId {
        let id = Cell::encode_cell_key(schema.to_id(&self.schemas), key);
        self.read_vertex(&id)
    }

    pub fn neighbourhoods<V, S>(&mut self, vertex: V, schema: S, ed: EdgeDirection)
        -> Result<Result<Vec<edge::Edge>, edge::EdgeError>, TxnError> where V: ToVertexId, S: ToSchemaId {
        let vertex_field = ed.as_field();
        let schema_id = schema.to_id(&self.schemas);
        let vertex_id = &vertex.to_id();
        match id_list::IdList::from_txn_and_container
            (self.neb_txn, vertex_id, vertex_field, schema_id).all()? {
            Err(e) => Ok(Err(edge::EdgeError::IdListError(e))),
            Ok(ids) => Ok(Ok({
                let mut edges = Vec::new();
                for id in ids {
                    match edge::from_id(
                        vertex_id, vertex_field, schema_id, &self.schemas, self.neb_txn, &id
                    )? {
                        Ok(e) => edges.push(e),
                        Err(er) => return Ok(Err(er))
                    }
                }
                edges
            }))
        }
    }
}