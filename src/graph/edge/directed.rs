use dovahkiin::types::Type;
use neb::ram::schema::Field;
use neb::ram::types::Id;
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction};

use super::{TEdge, EdgeType, EdgeError};
use super::bilateral::BilateralEdge;
use crate::server::schema::{SchemaContainer};
use crate::graph::fields::*;

lazy_static! {
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field::new(&*INBOUND_NAME, Type::Id, false, false, None),
            Field::new(&*OUTBOUND_NAME, Type::Id, false, false, None),
        ];
}

#[derive(Debug)]
pub struct DirectedEdge {
    inbound_id: Id,
    outbound_id: Id,
    schema_id: u32,
    pub cell: Option<dyn Cell>,
}

impl TEdge for DirectedEdge {
    type Edge = DirectedEdge;
    fn edge_type() -> EdgeType {
        EdgeType::Directed
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

    fn build_edge(a_field: Id, b_field: Id, schema_id: u32, cell: Option<dyn Cell>) -> Self::Edge {
        DirectedEdge {
            inbound_id: a_field,
            outbound_id: b_field,
            schema_id: schema_id,
            cell: cell
        }
    }

    fn edge_cell(&self) -> &Option<dyn Cell> {
        &self.cell
    }
    fn schema_id(&self) -> u32 {
        self.schema_id
    }
}

pub struct DirectedHyperEdge {
    inbound_ids: Vec<Id>,
    outbound_ids: Vec<Id>,
    cell: dyn Cell,
}

edge_index!(DirectedEdge);


