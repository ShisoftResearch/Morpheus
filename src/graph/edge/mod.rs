pub mod directed;
pub mod indirect;
pub mod hyper;

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
    Indirect,
    Hyper,
    Simple
}

pub enum EdgeError {
    WrongSchema,
    CannotFindSchema,
    TransactionError(TxnError),
    CellNotFound,
    WrongVertexField,
    WrongEdgeType,
    IdListError(IdListError)
}

pub trait TEdge {
    type Edge;

    fn edge_type() -> EdgeType;
    fn from_id(
        vertex_id: &Id, vertex_field: u64,
        schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id
    ) -> Result<Self::Edge, EdgeError>;
    fn link(
        from_id: &Id, to_id: &Id, body: Option<Cell>,
        txn: &mut Transaction,
        schemas: &Arc<SchemaContainer>
    ) -> Result<Self::Edge, EdgeError>;
    fn delete_edge(self, txn: &mut Transaction) -> Result<(), EdgeError>;
}