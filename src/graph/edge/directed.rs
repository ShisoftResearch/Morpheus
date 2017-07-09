use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Id, Value};
use neb::ram::cell::Cell;
use neb::client::transaction::{Transaction, TxnError};
use std::sync::Arc;

use super::{TEdge, EdgeType, EdgeError};
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

impl TEdge for DirectedEdge {
    type Edge = DirectedEdge;

    fn edge_type() -> EdgeType {
        EdgeType::Directed
    }
    fn from_id(
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
        let mut inbound_id = Id::unit_id();
        let mut outbound_id = Id::unit_id();
        let edge_cell = match cell_schema_type {
            SchemaType::Vertex => {
                let inbound_field = *INBOUND_KEY_ID;
                let outbound_field = *OUTBOUND_KEY_ID;
                match vertex_field {
                    inbound_field => {
                        inbound_id = *vertex_id;
                        outbound_id = *id;
                    },
                    outbound_field => {
                        outbound_id = *vertex_id;
                        inbound_id = *id;
                    },
                    _ => return Err(EdgeError::WrongVertexField)
                }
                None
            },
            SchemaType::Edge(EdgeType::Directed) => {
                if let (
                    &Value::Id(_inbound_id),
                    &Value::Id(_outbound_id)
                ) = (
                    &trace_cell.data[*INBOUND_KEY_ID],
                    &trace_cell.data[*OUTBOUND_KEY_ID]
                ) {
                    inbound_id = _inbound_id;
                    outbound_id = _outbound_id;
                }
                Some(trace_cell)
            },
            SchemaType::Edge(_) => return Err(EdgeError::WrongEdgeType),
            _ => return Err(EdgeError::WrongSchema)
        };
        Ok(DirectedEdge {
            inbound_id: inbound_id,
            outbound_id: outbound_id,
            cell: edge_cell
        })
    }
    fn link(
        from_id: &Id, to_id: &Id, body: Option<Cell>,
        txn: &mut Transaction,
        schemas: &Arc<SchemaContainer>
    ) -> Result<Self::Edge, EdgeError> {
        let mut from_vertex_pointer = Id::unit_id();
        let mut to_vertex_pointer = Id::unit_id();
        let edge_cell = if let Some(mut body_cell) = body {
            if schemas.schema_type(body_cell.header.schema) == Some(SchemaType::Edge(EdgeType::Directed)) {
                body_cell.header.set_id(&Id::rand());
                body_cell.data[*INBOUND_KEY_ID] = Value::Id(*from_id);
                body_cell.data[*OUTBOUND_KEY_ID] = Value::Id(*to_id);
                txn.write(&body_cell);
                from_vertex_pointer = body_cell.id();
                to_vertex_pointer = body_cell.id();
                Some(body_cell)
            } else { return Err(EdgeError::WrongSchema) }
        } else {
            from_vertex_pointer = *to_id;
            to_vertex_pointer = *from_id;
            None
        };
        IdList::from_txn_and_container(txn, from_id, *OUTBOUND_KEY_ID)
            .add(from_vertex_pointer).map_err(EdgeError::IdListError)?;
        IdList::from_txn_and_container(txn, to_id, *INBOUND_KEY_ID)
            .add(to_vertex_pointer).map_err(EdgeError::IdListError)?;
        Ok(DirectedEdge {
            inbound_id: *from_id,
            outbound_id: *to_id,
            cell: edge_cell
        })
    }
    fn delete_edge(self, txn: &mut Transaction) -> Result<(), EdgeError> {
        match self.cell {
            Some(cell) => {
                txn.remove(&cell.id()).map_err(EdgeError::TransactionError)?;
                IdList::from_txn_and_container(txn, &self.inbound_id, *OUTBOUND_KEY_ID)
                    .remove(&cell.id(), false).map_err(EdgeError::IdListError)?;
                IdList::from_txn_and_container(txn, &self.outbound_id, *INBOUND_KEY_ID)
                    .remove(&cell.id(), false).map_err(EdgeError::IdListError)?;
            },
            None => {
                IdList::from_txn_and_container(txn, &self.inbound_id, *OUTBOUND_KEY_ID)
                    .remove(&self.outbound_id, false).map_err(EdgeError::IdListError)?;
                IdList::from_txn_and_container(txn, &self.outbound_id, *INBOUND_KEY_ID)
                    .remove(&self.inbound_id, false).map_err(EdgeError::IdListError)?;
            }
        }
        Ok(())
    }
}