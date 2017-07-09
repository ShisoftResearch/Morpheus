use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, Value};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError};
use super::bilateral::BilateralEdge;
use super::super::vertex::{Vertex};
use super::super::id_list::IdList;
use server::schema::{SchemaContainer, SchemaType};
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

impl BilateralEdge for DirectedEdge {

    type Edge = DirectedEdge;

    fn edge_type() -> EdgeType {
        EdgeType::Directed
    }

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

    fn build_edge(a_field: Id, b_field: Id, cell: Option<Cell>) -> <Self as BilateralEdge>::Edge {
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