use neb::ram::schema::Field;
use neb::ram::cell::Cell;
use neb::ram::types::{TypeId, Id, key_hash, Map, Value};
use neb::client::transaction::{Transaction, TxnError};

pub const INBOUND_KEY: &'static str = "_inbound";
pub const OUTBOUND_KEY: &'static str = "_outbound";
pub const INDIRECTED_KEY: &'static str = "_indirected";

lazy_static! {
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&String::from(INBOUND_KEY), TypeId::Id as u32, false, false, None),
            Field::new(&String::from(OUTBOUND_KEY), TypeId::Id as u32, false, false, None),
            Field::new(&String::from(INDIRECTED_KEY), TypeId::Id as u32, false, false, None)
        ];
    pub static ref INBOUND_KEY_ID: u64 = key_hash(&String::from(INBOUND_KEY));
    pub static ref OUTBOUND_KEY_ID: u64 = key_hash(&String::from(OUTBOUND_KEY));
    pub static ref INDIRECTED_KEY_ID: u64 = key_hash(&String::from(INDIRECTED_KEY));
}

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