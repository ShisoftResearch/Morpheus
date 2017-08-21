#[macro_use]
mod macros;

pub mod directed;
pub mod undirectd;
pub mod hyper;
pub mod bilateral;

use std::ops::{Index, IndexMut};
use neb::ram::types::Id;
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use neb::dovahkiin::types::Value;
use graph::edge::bilateral::BilateralEdge;
use server::schema::{SchemaContainer, SchemaType};
use super::id_list::IdListError;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum EdgeType {
    Directed,
    Undirected
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct EdgeAttributes {
    pub edge_type: EdgeType,
    pub has_body: bool
}

impl EdgeAttributes {
    pub fn new(edge_type: EdgeType, has_body: bool) -> EdgeAttributes {
        EdgeAttributes {
            edge_type: edge_type,
            has_body: has_body
        }
    }
}

#[derive(Debug)]
pub enum EdgeError {
    WrongSchema,
    CannotFindSchema,
    CellNotFound,
    WrongVertexField,
    WrongEdgeType,
    IdListError(IdListError),
    SimpleEdgeShouldNotHaveBody,
    NormalEdgeShouldHaveBody,
    FilterEvalError(String)
}

pub trait TEdge {
    type Edge : TEdge;
    fn edge_type() -> EdgeType;
}

#[derive(Debug)]
pub enum Edge {
    Directed(directed::DirectedEdge),
    Undirected(undirectd::UndirectedEdge)
}

impl Edge {
    pub fn remove (self, txn: &mut Transaction)
        -> Result<Result<(), EdgeError>, TxnError> {
        match self {
            Edge::Directed(mut e) => e.remove(txn),
            Edge::Undirected(mut e) => e.remove(txn),
        }
    }
    pub fn get_data(&self) -> &Option<Cell> {
        match self {
            &Edge::Directed(ref e) => e.edge_cell(),
            &Edge::Undirected(ref e) => e.edge_cell(),
        }
    }
    pub fn one_oppisite_vertex_id(&self, vertex_id: &Id)  -> Option<&Id> {
        match self {
            &Edge::Directed(ref e) => e.oppisite_vertex_id(vertex_id),
            &Edge::Undirected(ref e) => e.oppisite_vertex_id(vertex_id),
        }
    }
}

impl Index<u64> for Edge {
    type Output = Value;
    fn index(&self, index: u64) -> &Self::Output {
        match self {
            &Edge::Directed(ref e) => &e[index],
            &Edge::Undirected(ref e) => &e[index],
        }
    }
}

impl <'a> Index<&'a str> for Edge {
    type Output = Value;
    fn index(&self, index: &'a str) -> &Self::Output {
        match self {
            &Edge::Directed(ref e) => &e[index],
            &Edge::Undirected(ref e) => &e[index],
        }
    }
}

impl <'a> IndexMut <&'a str> for Edge {
    fn index_mut<'b>(&'b mut self, index: &'a str) -> &'b mut Self::Output {
        match self {
            &mut Edge::Directed(ref mut e) => &mut e[index],
            &mut Edge::Undirected(ref mut e) => &mut e[index],
        }
    }
}

impl IndexMut<u64> for Edge {
    fn index_mut<'a>(&'a mut self, index: u64) -> &'a mut Self::Output {
        match self {
            &mut Edge::Directed(ref mut e) => &mut e[index],
            &mut Edge::Undirected(ref mut e) => &mut e[index],
        }
    }
}

pub fn from_id(
    vertex_id: &Id, vertex_field: u64, schema_id: u32,
    schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id
) -> Result<Result<Edge, EdgeError>, TxnError> {
    match schemas.schema_type(schema_id) {
        Some(SchemaType::Edge(ea)) => {
            match ea.edge_type {
                EdgeType::Directed => directed::DirectedEdge::from_id(
                    vertex_id, vertex_field, schema_id, schemas, txn, id
                ).map(|r| r.map(Edge::Directed)),
                EdgeType::Undirected => undirectd::UndirectedEdge::from_id(
                    vertex_id, vertex_field, schema_id, schemas, txn, id
                ).map(|r| r.map(Edge::Undirected))
            }
        },
        Some(_) => return Ok(Err(EdgeError::WrongSchema)),
        None => return Ok(Err(EdgeError::CannotFindSchema))
    }
}