use neb::ram::types::{Id, Value};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError};
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

    fn build_edge(a_field: Id, b_field: Id, cell: Option<Cell>) -> Self::Edge;
    fn edge_cell(&self) -> &Option<Cell>;

    fn from_id_(
        vertex_id: &Id, vertex_field: u64,
        schemas: &Arc<SchemaContainer>, txn: &mut Transaction, id: &Id
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
            SchemaType::Edge(edge_type) => {
                if edge_type == Self::edge_type() {
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
        Ok(Self::build_edge(a_id, b_id, edge_cell))
    }
    fn link_(
        vertex_a_id: &Id, vertex_b_id: &Id, body: Option<Cell>,
        txn: &mut Transaction,
        schemas: &Arc<SchemaContainer>
    ) -> Result<Self::Edge, EdgeError> {
        let mut vertex_a_pointer = Id::unit_id();
        let mut vertex_b_pointer = Id::unit_id();
        let edge_cell = if let Some(mut body_cell) = body {
            if schemas.schema_type(body_cell.header.schema) == Some(SchemaType::Edge(Self::edge_type())) {
                body_cell.header.set_id(&Id::rand());
                body_cell.data[Self::edge_a_field()] = Value::Id(*vertex_a_id);
                body_cell.data[Self::edge_b_field()] = Value::Id(*vertex_b_id);
                txn.write(&body_cell).map_err(EdgeError::TransactionError)?;
                vertex_a_pointer = body_cell.id();
                vertex_b_pointer = body_cell.id();
                Some(body_cell)
            } else { return Err(EdgeError::WrongSchema) }
        } else {
            vertex_a_pointer = *vertex_b_id;
            vertex_b_pointer = *vertex_a_id;
            None
        };
        IdList::from_txn_and_container(txn, vertex_a_id, Self::vertex_a_field())
            .add(&vertex_a_pointer).map_err(EdgeError::IdListError)?;
        IdList::from_txn_and_container(txn, vertex_b_id, Self::vertex_b_field())
            .add(&vertex_b_pointer).map_err(EdgeError::IdListError)?;
        Ok(Self::build_edge(*vertex_a_id, *vertex_b_id, edge_cell))
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
        IdList::from_txn_and_container(txn, self.vertex_a(), Self::vertex_a_field())
            .remove(&v_a_removal, false).map_err(EdgeError::IdListError)?;
        IdList::from_txn_and_container(txn, self.vertex_b(), Self::vertex_b_field())
            .remove(&v_b_removal, false).map_err(EdgeError::IdListError)?;
        Ok(())
    }
}