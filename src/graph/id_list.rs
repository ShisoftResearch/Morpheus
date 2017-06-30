use neb::ram::schema::Field;
use neb::ram::cell::MAX_CELL_SIZE;
use neb::ram::types::{TypeId, Id, id_io, u32_io, key_hash};
use neb::client::transaction::{Transaction, TxnError};

use graph::vertex::Vertex;

pub const NEXT_KEY: &'static str = "_next";
pub const LIST_KEY: &'static str = "_list";

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field::new(&String::from(NEXT_KEY), TypeId::Id as u32, true, false, None),
            Field::new(&String::from(LIST_KEY), TypeId::Id as u32, false, true, None)
          ];
    pub static ref LIST_CAPACITY: usize =
        ((MAX_CELL_SIZE - u32_io::size(0) - id_io::size(0)) / id_io::size(0));
    pub static ref NEXT_KEY_ID: u64 = key_hash(&String::from(NEXT_KEY));
}

pub struct IdList<'a> {
    txn: &'a mut Transaction,
    id: Id,
    field_id: u64
}

impl<'a> IdList <'a> {
    pub fn from_txn_vertex(txn: &'a mut Transaction, id: &Id, field_id: u64) -> IdList<'a> {
        IdList {
            txn: txn,
            id: *id,
            field_id: field_id
        }
    }
    pub fn into_iter(self) -> IdListIntoIterator<'a> {
        IdListIntoIterator {
            list: self,
            index: 0
        }
    }
}

pub struct IdListIntoIterator<'a> {
    list: IdList<'a>,
    index: usize,
}

impl <'a>Iterator for IdListIntoIterator<'a> {

    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }

    fn count(self) -> usize where Self: Sized {
        unimplemented!()
    }

    fn last(self) -> Option<Self::Item> where Self: Sized {
        unimplemented!()
    }

    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        unimplemented!()
    }
}

