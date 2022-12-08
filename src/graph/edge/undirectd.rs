use dovahkiin::types::Type;
use neb::ram::schema::Field;
use neb::ram::types::{Id, key_hash};
use neb::ram::cell::{Cell, SharedCell};
use dovahkiin::types::SharedValue;

use super::{TEdge, EdgeType};
use super::bilateral::BilateralEdge;
use crate::graph::fields::*;



lazy_static! {
    pub static ref EDGE_VERTEX_A_NAME: String = String::from("_vertex_a");
    pub static ref EDGE_VERTEX_B_NAME: String = String::from("_vertex_b");
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field::new(&*EDGE_VERTEX_A_NAME, Type::Id, false, false, None),
            Field::new(&*EDGE_VERTEX_B_NAME, Type::Id, false, false, None),
    ];
    pub static ref EDGE_VERTEX_A_ID: u64 = key_hash(&*EDGE_VERTEX_A_NAME);
    pub static ref EDGE_VERTEX_B_ID: u64 = key_hash(&*EDGE_VERTEX_B_NAME);
}

#[derive(Debug)]
pub struct UndirectedEdge<'a> {
    vertex_a_id: Id,
    vertex_b_id: Id,
    schema_id: u32,
    cell: Option<SharedCell<'a>>,
}

impl <'a> TEdge for UndirectedEdge<'a> {
    type Edge = UndirectedEdge<'a>;
    fn edge_type() -> EdgeType {
        EdgeType::Undirected
    }
}

impl <'a> BilateralEdge for UndirectedEdge<'a> {

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

    fn build_edge(a_field: Id, b_field: Id, schema_id: u32, cell: Option<SharedCell<'a>>) -> Self::Edge {
        UndirectedEdge {
            vertex_a_id: a_field,
            vertex_b_id: b_field,
            schema_id: schema_id,
            cell: cell
        }
    }

    fn edge_cell(&self) -> &Option<SharedCell<'a>> {
        &self.cell
    }
    fn schema_id(&self) -> u32 {
        self.schema_id
    }
}

edge_index!(UndirectedEdge);