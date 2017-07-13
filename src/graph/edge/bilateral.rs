use neb::ram::types::{Id, Value};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use neb::utils::rand;
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError, EdgeAttributes};
use super::super::vertex::{Vertex};
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

    fn from_id_(
        vertex_id: &Id, vertex_field: u64,
        schema_id: u32, schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id
    ) -> Result<Self::Edge, EdgeError> {
        let trace_cell = match txn.read(id) {
            Ok(Some(cell)) => cell,
            Ok(None) => return Err(EdgeError::CellNotFound),
            Err(e) => return Err(EdgeError::TransactionError(e))
        };
        let cell_schema_type = match schemas.schema_type(trace_cell.header.schema) {
            Some(t) => t, None => return Err(EdgeError::CannotFindSchema)
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
                    return Err(EdgeError::WrongVertexField);
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
                    return Err(EdgeError::WrongEdgeType)
                }
            },
            _ => return Err(EdgeError::WrongSchema)
        };
        Ok(Self::build_edge(a_id, b_id, schema_id, edge_cell))
    }
    fn link_(
        vertex_a_id: &Id, vertex_b_id: &Id, body: Option<Cell>,
        txn: &mut Transaction,
        schema_id: u32, schemas: &Arc<SchemaContainer>
    ) -> Result<Self::Edge, EdgeError> {
        let mut vertex_a_pointer = Id::unit_id();
        let mut vertex_b_pointer = Id::unit_id();
        let edge_cell = {
            match schemas.schema_type(schema_id) {
                Some(SchemaType::Edge(ea)) => {
                    if ea.edge_type != Self::edge_type() { return Err(EdgeError::WrongEdgeType); }
                    if ea.has_body {
                        if let Some(mut body_cell) = body {
                            body_cell.header.set_id(&Id::new(vertex_a_id.higher, rand::next()));
                            body_cell.header.schema = schema_id;
                            body_cell.data[Self::edge_a_field()] = Value::Id(*vertex_a_id);
                            body_cell.data[Self::edge_b_field()] = Value::Id(*vertex_b_id);
                            txn.write(&body_cell).map_err(EdgeError::TransactionError)?;
                            vertex_a_pointer = body_cell.id();
                            vertex_b_pointer = body_cell.id();
                            Some(body_cell)
                        } else {
                            return Err(EdgeError::NormalEdgeShouldHaveBody);
                        }
                    } else {
                        if body.is_none() {
                            vertex_a_pointer = *vertex_b_id;
                            vertex_b_pointer = *vertex_a_id;
                            None
                        } else {
                            return Err(EdgeError::SimpleEdgeShouldHaveNoBody);
                        }
                    }
                },
                Some(_) => return Err(EdgeError::WrongSchema),
                None => return Err(EdgeError::CannotFindSchema)
            }
        };
        IdList::from_txn_and_container(txn, vertex_a_id, Self::vertex_a_field(), schema_id)
            .add(&vertex_a_pointer).map_err(EdgeError::IdListError)?;
        IdList::from_txn_and_container(txn, vertex_b_id, Self::vertex_b_field(), schema_id)
            .add(&vertex_b_pointer).map_err(EdgeError::IdListError)?;
        Ok(Self::build_edge(*vertex_a_id, *vertex_b_id, schema_id, edge_cell))
    }
    fn delete_edge_(&mut self, txn: &mut Transaction) -> Result<(), EdgeError> {
        let (v_a_removal, v_b_removal) = match self.edge_cell() {
            &Some(ref cell) => {
                txn.remove(&cell.id()).map_err(EdgeError::TransactionError)?;
                (cell.id(), cell.id())
            },
            &None => {
                (*self.vertex_b(), *self.vertex_a())
            }
        };
        let schema_id = self.schema_id();
        IdList::from_txn_and_container(txn, self.vertex_a(), Self::vertex_a_field(), self.schema_id())
            .remove(&v_a_removal, false).map_err(EdgeError::IdListError)?;
        IdList::from_txn_and_container(txn, self.vertex_b(), Self::vertex_b_field(), self.schema_id())
            .remove(&v_b_removal, false).map_err(EdgeError::IdListError)?;
        Ok(())
    }
}