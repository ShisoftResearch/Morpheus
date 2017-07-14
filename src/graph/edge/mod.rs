pub mod directed;
pub mod undirectd;
pub mod hyper;
pub mod bilateral;

use neb::ram::types::Id;
use neb::client::transaction::{Transaction, TxnError};
use neb::ram::cell::Cell;
use graph::vertex::Vertex;
use server::schema::{MorpheusSchema, SchemaContainer};
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

pub enum EdgeError {
    WrongSchema,
    CannotFindSchema,
    CellNotFound,
    WrongVertexField,
    WrongEdgeType,
    IdListError(IdListError),
    SimpleEdgeShouldNotHaveBody,
    NormalEdgeShouldHaveBody
}

pub trait TEdge {
    type Edge : TEdge;
    fn edge_type() -> EdgeType;
}

pub enum Edge {
    Directed(directed::DirectedEdge),
    Undirected(undirectd::UndirectedEdge)
}