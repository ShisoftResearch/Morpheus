use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, key_hash};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction};
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError};
use super::bilateral::BilateralEdge;
use server::schema::{SchemaContainer};
use graph::fields::*;



lazy_static! {
    pub static ref EDGE_VERTEX_A_NAME: String = String::from("_vertex_a");
    pub static ref EDGE_VERTEX_B_NAME: String = String::from("_vertex_b");
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field::new(&*EDGE_VERTEX_A_NAME, TypeId::Id as u32, false, false, None),
            Field::new(&*EDGE_VERTEX_B_NAME, TypeId::Id as u32, false, false, None),
    ];
    pub static ref EDGE_VERTEX_A_ID: u64 = key_hash(&*EDGE_VERTEX_A_NAME);
    pub static ref EDGE_VERTEX_B_ID: u64 = key_hash(&*EDGE_VERTEX_B_NAME);
}

pub struct UndirectedEdge {
    vertex_a_id: Id,
    vertex_b_id: Id,
    schema_id: u32,
    cell: Option<Cell>,
}

impl TEdge for UndirectedEdge {
    type Edge = UndirectedEdge;
    fn edge_type() -> EdgeType {
        EdgeType::Undirected
    }
}

impl BilateralEdge for UndirectedEdge {

    fn vertex_a_field() -> u64 {
        *UNDIRECTED_KEY_ID
    }

    fn vertex_b_field() -> u64 {
        *UNDIRECTED_KEY_ID
    }

    fn vertex_a(&self) -> &Id {
        &self.vertex_a_id
    }

    fn vertex_b(&self) -> &Id {
        &self.vertex_b_id
    }

    fn edge_a_field() -> u64 {
        *EDGE_VERTEX_A_ID
    }

    fn edge_b_field() -> u64 {
        *EDGE_VERTEX_B_ID
    }

    fn build_edge(a_field: Id, b_field: Id, schema_id: u32, cell: Option<Cell>) -> Self::Edge {
        UndirectedEdge {
            vertex_a_id: a_field,
            vertex_b_id: b_field,
            schema_id: schema_id,
            cell: cell
        }
    }

    fn edge_cell(&self) -> &Option<Cell> {
        &self.cell
    }
    fn schema_id(&self) -> u32 {
        self.schema_id
    }
}

edge_index!(UndirectedEdge);