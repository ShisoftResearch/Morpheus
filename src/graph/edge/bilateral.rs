use neb::ram::types::{Id, Map};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use neb::utils::rand;
use neb::dovahkiin::types::Value;
use std::sync::Arc;

use super::{TEdge, EdgeError};
use super::super::id_list::IdList;
use server::schema::{SchemaContainer, SchemaType};


pub trait BilateralEdge : TEdge {

    fn vertex_a_field() -> u64;
    fn vertex_b_field() -> u64;

    fn vertex_a(&self) -> &Id;
    fn vertex_b(&self) -> &Id;

    fn edge_a_field() -> u64;
    fn edge_b_field() -> u64;

    fn build_edge(a_field: Id, b_field: Id, schema_id: u32, cell: Option<Cell>) -> Self::Edge;
    fn edge_cell(&self) -> &Option<Cell>;
    fn schema_id(&self) -> u32;

    fn from_id(
        vertex_id: &Id, vertex_field: u64,
        schema_id: u32, schemas: &Arc<SchemaContainer>, txn: &Transaction, id: &Id
    ) -> Result<Result<Self::Edge, EdgeError>, TxnError> {
        let trace_cell = match txn.read(id)? {
            Some(cell) => cell,
            None => return Ok(Err(EdgeError::CellNotFound))
        };
        let cell_schema_type = match schemas.schema_type(trace_cell.header.schema) {
            Some(t) => t, None => return Ok(Err(EdgeError::CannotFindSchema))
        };
        let mut a_id = Id::unit_id();
        let mut b_id = Id::unit_id();
        let edge_cell = match cell_schema_type {
            SchemaType::Vertex => {
                if vertex_field == Self::vertex_a_field() {
                    a_id = *vertex_id;
                    b_id = *id;
                } else if vertex_field == Self::vertex_b_field() {
                    b_id = *vertex_id;
                    a_id = *id;
                } else {
                    return Ok(Err(EdgeError::WrongVertexField));
                }
                None
            },
            SchemaType::Edge(edge_attrs) => {
                if edge_attrs.edge_type == Self::edge_type() {
                    if let (
                        &Value::Id(e_a_id),
                        &Value::Id(e_b_id)
                    ) = (
                        &trace_cell.data[Self::edge_a_field()],
                        &trace_cell.data[Self::edge_b_field()]
                    ) {
                        a_id = e_a_id;
                        b_id = e_b_id;
                    }
                    Some(trace_cell)
                } else {
                    return Ok(Err(EdgeError::WrongEdgeType))
                }
            },
            _ => return Ok(Err(EdgeError::WrongSchema))
        };
        Ok(Ok(Self::build_edge(a_id, b_id, schema_id, edge_cell)))
    }
    fn link(
        vertex_a_id: &Id, vertex_b_id: &Id, body: Option<Map>,
        txn: &Transaction,
        schema_id: u32, schemas: &Arc<SchemaContainer>
    ) -> Result<Result<Self::Edge, EdgeError>, TxnError> {
        let mut vertex_a_pointer = Id::unit_id();
        let mut vertex_b_pointer = Id::unit_id();
        let edge_cell = {
            match schemas.schema_type(schema_id) {
                Some(SchemaType::Edge(ea)) => {
                    if ea.edge_type != Self::edge_type() { return Ok(Err(EdgeError::WrongEdgeType)); }
                    if ea.has_body {
                        if let Some(body_map) = body {
                            let mut edge_body_cell = Cell::new_with_id(
                                schema_id,
                                &Id::new(vertex_a_id.higher, rand::next()),
                                Value::Map(body_map)
                            );
                            edge_body_cell.data[Self::edge_a_field()] = Value::Id(*vertex_a_id);
                            edge_body_cell.data[Self::edge_b_field()] = Value::Id(*vertex_b_id);
                            txn.write(&edge_body_cell)?;
                            vertex_a_pointer = edge_body_cell.id();
                            vertex_b_pointer = edge_body_cell.id();
                            Some(edge_body_cell)
                        } else {
                            return Ok(Err(EdgeError::NormalEdgeShouldHaveBody));
                        }
                    } else {
                        if body.is_none() {
                            vertex_a_pointer = *vertex_b_id;
                            vertex_b_pointer = *vertex_a_id;
                            None
                        } else {
                            return Ok(Err(EdgeError::SimpleEdgeShouldNotHaveBody));
                        }
                    }
                },
                Some(_) => return Ok(Err(EdgeError::WrongSchema)),
                None => return Ok(Err(EdgeError::CannotFindSchema))
            }
        };
        match IdList::from_txn_and_container(txn, vertex_a_id, Self::vertex_a_field(), schema_id)
            .add(&vertex_a_pointer)?.map_err(EdgeError::IdListError) {
            Err(e) => return Ok(Err(e)), _ => {}
        }
        match IdList::from_txn_and_container(txn, vertex_b_id, Self::vertex_b_field(), schema_id)
            .add(&vertex_b_pointer)?.map_err(EdgeError::IdListError) {
            Err(e) => return Ok(Err(e)), _ => {}
        }
        Ok(Ok(Self::build_edge(*vertex_a_id, *vertex_b_id, schema_id, edge_cell)))
    }
    fn remove(&mut self, txn: &Transaction) -> Result<Result<(), EdgeError>, TxnError> {
        let (v_a_removal, v_b_removal) = match self.edge_cell() {
            &Some(ref cell) => {
                txn.remove(&cell.id())?;
                (cell.id(), cell.id())
            },
            &None => {
                (*self.vertex_b(), *self.vertex_a())
            }
        };
        match IdList::from_txn_and_container(txn, self.vertex_a(), Self::vertex_a_field(), self.schema_id())
            .remove(&v_a_removal, false)?.map_err(EdgeError::IdListError) {
            Err(e) => return Ok(Err(e)), _ => {}
        }
        match IdList::from_txn_and_container(txn, self.vertex_b(), Self::vertex_b_field(), self.schema_id())
            .remove(&v_b_removal, false)?.map_err(EdgeError::IdListError) {
            Err(e) => return Ok(Err(e)), _ => {}
        }
        Ok(Ok(()))
    }
    fn oppisite_vertex_id(&self, vertex_id: &Id) -> Option<&Id> {
        let v1_id = self.vertex_a();
        let v2_id = self.vertex_b();
        if v1_id == vertex_id {
            Some(v2_id)
        } else if v2_id == vertex_id {
            Some(v1_id)
        } else {
            None
        }
    }
}

