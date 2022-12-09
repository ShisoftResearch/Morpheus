#[macro_use]
mod macros;

pub mod bilateral;
pub mod directed;
pub mod hyper;
pub mod undirectd;

use super::id_list::IdListError;
use crate::graph::edge::bilateral::BilateralEdge;
use crate::server::schema::{SchemaContainer, GraphSchema};
use dovahkiin::types::{OwnedValue, SharedValue, Value};
use neb::client::transaction::{Transaction, TxnError};
use neb::ram::cell::{Cell, OwnedCell, SharedCell};
use neb::ram::types::Id;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum EdgeType {
    Directed,
    Undirected,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct EdgeAttributes {
    pub edge_type: EdgeType,
    pub has_body: bool,
}

impl EdgeAttributes {
    pub fn new(edge_type: EdgeType, has_body: bool) -> EdgeAttributes {
        EdgeAttributes {
            edge_type: edge_type,
            has_body: has_body,
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
    FilterEvalError(String),
}

pub trait TEdge {
    type Edge: TEdge;
    fn edge_type() -> EdgeType;
}

#[derive(Debug)]
pub enum Edge {
    Directed(directed::DirectedEdge),
    Undirected(undirectd::UndirectedEdge),
}

impl Edge {
    pub async fn remove(self, txn: &Transaction) -> Result<Result<(), EdgeError>, TxnError> {
        match self {
            Edge::Directed(mut e) => e.remove(txn).await,
            Edge::Undirected(mut e) => e.remove(txn).await,
        }
    }
    pub async fn get_data(&self) -> &Option<OwnedCell> {
        match self {
            &Edge::Directed(ref e) => e.edge_cell(),
            &Edge::Undirected(ref e) => e.edge_cell(),
        }
    }
    pub async fn one_opposite_id_vertex_id(&self, vertex_id: &Id) -> Option<&Id> {
        match self {
            &Edge::Directed(ref e) => e.oppisite_vertex_id(vertex_id),
            &Edge::Undirected(ref e) => e.oppisite_vertex_id(vertex_id),
        }
    }
}

impl Index<u64> for Edge {
    type Output = OwnedValue;
    fn index(&self, index: u64) -> &Self::Output {
        match self {
            &Edge::Directed(ref e) => &e[index],
            &Edge::Undirected(ref e) => &e[index],
        }
    }
}

impl<'a> Index<&'a str> for Edge {
    type Output = OwnedValue;
    fn index(&self, index: &'a str) -> &Self::Output {
        match self {
            &Edge::Directed(ref e) => &e[index],
            &Edge::Undirected(ref e) => &e[index],
        }
    }
}

impl<'a> IndexMut<&'a str> for Edge {
    fn index_mut(&mut self, index: &'a str) -> &mut Self::Output {
        match self {
            &mut Edge::Directed(ref mut e) => &mut e[index],
            &mut Edge::Undirected(ref mut e) => &mut e[index],
        }
    }
}

impl IndexMut<u64> for Edge {
    fn index_mut(&mut self, index: u64) -> &mut Self::Output {
        match self {
            &mut Edge::Directed(ref mut e) => &mut e[index],
            &mut Edge::Undirected(ref mut e) => &mut e[index],
        }
    }
}

pub async fn from_id(
    vertex_id: Id,
    vertex_field: u64,
    schema_id: u32,
    schemas: &Arc<SchemaContainer>,
    txn: &Transaction,
    id: Id,
) -> Result<Result<Edge, EdgeError>, TxnError> {
    match schemas.schema_type(schema_id) {
        Some(GraphSchema::Edge(ea)) => match ea.edge_type {
            EdgeType::Directed => directed::DirectedEdge::from_id(
                vertex_id,
                vertex_field,
                schema_id,
                schemas,
                txn,
                id,
            )
            .await
            .map(|r| r.map(Edge::Directed)),
            EdgeType::Undirected => undirectd::UndirectedEdge::from_id(
                vertex_id,
                vertex_field,
                schema_id,
                schemas,
                txn,
                id,
            )
            .await
            .map(|r| r.map(Edge::Undirected)),
        },
        Some(_) => return Ok(Err(EdgeError::WrongSchema)),
        None => return Ok(Err(EdgeError::CannotFindSchema)),
    }
}
