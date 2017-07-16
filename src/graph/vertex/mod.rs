use neb::ram::cell::Cell;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::client::transaction::{Transaction, TxnError};
use graph::fields::*;
use graph::id_list::{IdList, IdListError};
use graph::edge;
use server::schema::SchemaContainer;

use std::sync::Arc;
use super::EdgeDirection;

pub struct Vertex {
    pub cell: Cell
}

pub enum RemoveError {
    NotFound,
    FormatError,
    IdListError(IdListError),
    EdgeError(edge::EdgeError)
}

pub fn cell_to_vertex(cell: Cell) -> Vertex {
    Vertex {
        cell: cell
    }
}

pub fn vertex_to_cell(vertex: Vertex) -> Cell {
    vertex.cell
}

impl Vertex {
    pub fn new(schema: u32, data: Map) -> Vertex {
        Vertex {
            cell: Cell::new_with_id(schema, &Id::unit_id(), Value::Map(data))
        }
    }
    pub fn schema(&self) -> u32 {
        self.cell.header.schema
    }
}

pub fn txn_remove(txn: &mut Transaction, schemas: &Arc<SchemaContainer>, id: &Id) -> Result<Result<(), RemoveError>, TxnError> {
    match txn.read(id)? {
        Some(cell) => {
            let remove_field_lists = |id: &Id, txn: &mut Transaction, field_id: u64|
                -> Result<Result<(), RemoveError>, TxnError> {
                let (type_list_id, schemas_ids) = match IdList::cell_types(txn, id, field_id)? {
                    Some(t) => t, None => return Ok(Err(RemoveError::FormatError))
                };
                for schema_id in schemas_ids {
                    let mut id_list = IdList::from_txn_and_container(txn, id, field_id, schema_id);
                    {                          // remove edge cells
                        let mut iter = match id_list.iter()? {
                            Ok(iter) => iter, Err(e) => return Ok(Err(RemoveError::IdListError(e)))
                        };
                        while let Some(edge_id) = iter.next() {
                            let edge = match edge::from_id(
                                id, field_id, schema_id, schemas,
                                iter.segments.id_iter.txn, &edge_id
                            )? {
                                Ok(edge) => edge, Err(e) => return Ok(Err(RemoveError::EdgeError(e)))
                            };
                            match edge.remove(iter.segments.id_iter.txn)? {
                                Ok(()) => {}, Err(e) => return Ok(Err(RemoveError::EdgeError(e)))
                            }
                        }
                    }
                    match id_list.clear_segments()? { // remove segment cells
                        Ok(()) => {}, Err(e) => return Ok(Err(RemoveError::IdListError(e)))
                    }
                }
                txn.remove(&type_list_id)?; // remove field schema list cell
                Ok(Ok(()))
            };
            remove_field_lists(id, txn, EdgeDirection::Undirected.as_field());
            remove_field_lists(id, txn, EdgeDirection::Outbound.as_field());
            remove_field_lists(id, txn, EdgeDirection::Inbound.as_field());
            txn.remove(id).map(|_| Ok(())) // remove vertex cell
        },
        None => Ok(Err(RemoveError::NotFound))
    }
}

pub fn txn_update<U>(txn: &mut Transaction, id: &Id, update: &U) -> Result<(), TxnError>
    where U: Fn(Vertex) -> Option<Vertex> {
    let update_cell = |cell: Cell| {
        match update(cell_to_vertex(cell)) {
            Some(vertex) => Some(vertex_to_cell(vertex)),
            None => None
        }
    };
    let cell = txn.read(id)?;
    match cell {
        Some(cell) => {
            match update_cell(cell) {
                Some(cell) => txn.update(&cell),
                None => txn.abort()
            }
        },
        None => txn.abort()
    }
}