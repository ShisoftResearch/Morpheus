use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction};
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError};
use super::bilateral::BilateralEdge;
use server::schema::{SchemaContainer};
use graph::fields::*;


lazy_static! {
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field::new(&*INBOUND_NAME, TypeId::Id as u32, false, false, None),
            Field::new(&*OUTBOUND_NAME, TypeId::Id as u32, false, false, None),
        ];
}

pub struct DirectedEdge {
    inbound_id: Id,
    outbound_id: Id,
    cell: Option<Cell>,
}

impl TEdge for DirectedEdge {

    type Edge = DirectedEdge;

    fn edge_type() -> EdgeType {
        EdgeType::Directed
    }
    fn from_id(vertex_id: &Id, vertex_field: u64, schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id) -> Result<Self::Edge, EdgeError> {
        Self::from_id_(vertex_id, vertex_field, schemas, txn, id)
    }

    fn link(vertex_a_id: &Id, vertex_b_id: &Id, body: Option<Cell>, txn: &mut Transaction, schemas: &Arc<SchemaContainer>) -> Result<Self::Edge, EdgeError> {
        Self::link_(vertex_a_id, vertex_b_id, body,txn, schemas)
    }

    fn delete_edge(&mut self, txn: &mut Transaction) -> Result<(), EdgeError> {
        self.delete_edge_(txn)
    }
}

impl BilateralEdge for DirectedEdge {

    fn vertex_a_field() -> u64 {
        *OUTBOUND_KEY_ID
    }

    fn vertex_b_field() -> u64 {
        *INBOUND_KEY_ID
    }

    fn vertex_a(&self) -> &Id {
        &self.inbound_id
    }

    fn vertex_b(&self) -> &Id {
        &self.outbound_id
    }

    fn edge_a_field() -> u64 {
        *INBOUND_KEY_ID
    }

    fn edge_b_field() -> u64 {
        *OUTBOUND_KEY_ID
    }

    fn build_edge(a_field: Id, b_field: Id, cell: Option<Cell>) -> Self::Edge {
        DirectedEdge {
            inbound_id: a_field,
            outbound_id: b_field,
            cell: cell
        }
    }

    fn edge_cell(&self) -> &Option<Cell> {
        &self.cell
    }
}