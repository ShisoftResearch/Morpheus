use neb::ram::cell::Cell;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::client::transaction::{Transaction, TxnError};
use graph::fields::*;

pub struct Vertex {
    pub id: Id,
    pub schema: u32,
    pub data: Value,
}

pub fn cell_to_vertex(cell: Cell) -> Vertex {
    Vertex {
        id: cell.header.id(),
        schema: cell.header.schema,
        data: cell.data
    }
}

pub fn vertex_to_cell(vertex: Vertex) -> Cell {
    Cell::new_with_id(vertex.schema, &vertex.id, vertex.data)
}

impl Vertex {
    pub fn new(schema: u32, data: Map) -> Vertex {
        Vertex {
            id: Id::unit_id(),
            schema: schema,
            data: Value::Map(data)
        }
    }
}

pub fn txn_remove(txn: &mut Transaction, id: &Id) -> Result<(), TxnError> {
    match txn.read(id)? {
        Some(cell) => {
            let vertex = cell_to_vertex(cell); // for remove it from neighbourhoods
            txn.remove(id)
        },
        None => txn.abort()
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