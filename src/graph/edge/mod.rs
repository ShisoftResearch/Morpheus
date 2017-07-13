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
    TransactionError(TxnError),
    CellNotFound,
    WrongVertexField,
    WrongEdgeType,
    IdListError(IdListError),
    SimpleEdgeShouldHaveNoBody,
    NormalEdgeShouldHaveBody
}

pub trait TEdge {

    type Edge : TEdge;

    fn edge_type() -> EdgeType;
    fn from_id(
        vertex_id: &Id, vertex_field: u64,
        schema_id: u32, schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id
    ) -> Result<Self::Edge, EdgeError>;
    fn link(
        vertex_a_id: &Id, vertex_b_id: &Id, body: Option<Cell>,
        txn: &mut Transaction,
        schema_id: u32, schemas: &Arc<SchemaContainer>
    ) -> Result<Self::Edge, EdgeError>;
    fn delete_edge(&mut self, txn: &mut Transaction) -> Result<(), EdgeError>;
}