use dovahkiin::types::{SharedMap, SharedValue};
use neb::ram::types::{Id, key_hash};
use neb::client::transaction::{Transaction, TxnError};
use neb::dovahkiin::types::Value;
use neb::ram::cell::{SharedCell};
use crate::graph::id_list::{IdList, IdListError};
use crate::graph::edge;
use crate::server::schema::SchemaContainer;

use std::ops::{Index, IndexMut};
use std::sync::Arc;
use super::EdgeDirection;

#[derive(Debug)]
pub struct Vertex<'a> {
    pub cell: SharedCell<'a>
}

pub enum RemoveError {
    NotFound,
    FormatError,
    IdListError(IdListError),
    EdgeError(edge::EdgeError)
}

pub fn cell_to_vertex<'a>(cell: SharedCell<'a>) -> Vertex<'a> {
    Vertex {
        cell
    }
}

pub fn vertex_to_cell<'a>(vertex: Vertex<'a>) -> SharedCell<'a> {
    vertex.cell
}

impl <'a> Vertex <'a> {
    pub fn new(schema: u32, data: SharedMap) -> Vertex {
        Vertex {
            cell: SharedCell::new_with_id(schema, &Id::unit_id(), Value::Map(data))
        }
    }
    pub fn schema(&self) -> u32 {
        self.cell.header.schema
    }
}

pub fn txn_remove<V>(txn: &Transaction, schemas: &Arc<SchemaContainer>, vertex: V)
    -> Result<Result<(), RemoveError>, TxnError> where V: ToVertexId {
    let id = &vertex.to_id();
    match txn.read(id)? {
        Some(cell) => {
            let remove_field_lists = |id: &Id, txn: &Transaction, field_id: u64|
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
            match remove_field_lists(id, txn, EdgeDirection::Undirected.as_field())? {
                Ok(()) => {}, Err(e) => return Ok(Err(e))
            }
            match remove_field_lists(id, txn, EdgeDirection::Inbound.as_field())? {
                Ok(()) => {}, Err(e) => return Ok(Err(e))
            }
            match remove_field_lists(id, txn, EdgeDirection::Outbound.as_field())? {
                Ok(()) => {}, Err(e) => return Ok(Err(e))
            }
            txn.remove(id).map(|_| Ok(())) // remove vertex cell
        },
        None => Ok(Err(RemoveError::NotFound))
    }
}

pub fn txn_update<U, V>(txn: &Transaction, vertex: V, update: &U) -> Result<(), TxnError>
    where V: ToVertexId, U: Fn(Vertex) -> Option<Vertex> {
    let id = &vertex.to_id();
    let update_cell = |cell| {
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

pub trait ToVertexId {
    fn to_id(&self) -> Id;
}

impl <'a> ToVertexId for Vertex<'a> {
    fn to_id(&self) -> Id {
        self.cell.id()
    }
}

impl ToVertexId for Id {
    fn to_id(&self) -> Id {
        *self
    }
}

impl <'a> ToVertexId for &'a Id {
    fn to_id(&self) -> Id {
        **self
    }
}

impl <'a, 'b> ToVertexId for &'a Vertex<'b> {
    fn to_id(&self) -> Id {
        self.cell.id()
    }
}

impl <'a> Index<u64> for Vertex<'a> {
    type Output = SharedValue<'a>;
    fn index(&self, index: u64) -> &Self::Output {
        &self.cell.data[index]
    }
}

impl <'a, 'b> Index<&'a str> for Vertex<'b> {
    type Output = SharedValue<'b>;
    fn index(&self, index: &'a str) -> &Self::Output {
        &self.cell.data[index]
    }
}

impl <'a, 'b> IndexMut <&'a str> for Vertex<'b> {
    fn index_mut(&mut self, index: &'a str) -> &mut Self::Output {
        &mut self.cell[index]
    }
}

impl <'a> IndexMut<u64> for Vertex<'a> {
    fn index_mut(&mut self, index: u64) -> &mut Self::Output {
        &mut self.cell[index]
    }
}