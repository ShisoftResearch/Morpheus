use neb::ram::schema::Field;
use neb::ram::cell::{MAX_CELL_SIZE, Cell};
use neb::ram::types::{TypeId, Id, Map, Value, id_io, u32_io, key_hash};
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
    pub static ref LIST_KEY_ID: u64 = key_hash(&String::from(LIST_KEY));
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
            field_id: field_id,
        }
    }
}

pub struct IdListSegmentIntoIterator<'a> {
    txn: &'a mut Transaction,
    next: Id
}

impl <'a>IdListSegmentIntoIterator<'a> {
    pub fn new(txn: &'a mut Transaction, head_id: Id) -> IdListSegmentIntoIterator<'a> {
        IdListSegmentIntoIterator {
            txn: txn,
            next: head_id
        }
    }
}

impl <'a> Iterator for IdListSegmentIntoIterator<'a> {

    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let mut id_set = false;
        if !self.next.is_unit_id() {
            match self.txn.read(&self.next) {
                Ok(Some(cell)) => {
                    if let Value::Map(ref map) = cell.data {
                        if let &Value::Id(ref id) = map.get_by_key_id(*NEXT_KEY_ID) {
                            self.next = *id;
                            id_set = true;
                        }
                    }
                    if id_set {
                        return Some(cell)
                    }
                },
                _ => {}
            }
        }
        None
    }
}

pub struct IdListIterator<'a> {
    segments: IdListSegmentIntoIterator<'a>,
    current_seg: Option<Cell>,
    current_pos: u32
}

impl <'a> IdListIterator <'a> {
    pub fn next_seg(&mut self) {
        self.current_seg = self.segments.next();
        self.current_pos = 0;
    }
}

impl <'a> Iterator for IdListIterator<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        let mut need_next_seg = false;
        if let Some(ref cell) = self.current_seg {
            let pos = self.current_pos;
            self.current_pos += 1;
            if let &Value::Map(ref map) = &cell.data {
                if let &Value::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    if let Some(&Value::Id(id)) = list.get(pos as usize) {
                        return Some(id);
                    } else {
                        need_next_seg = true
                    }
                }
            }
        };
        if need_next_seg {
            self.next_seg();
            self.next()
        } else {
            None
        }
    }

    fn last(self) -> Option<Self::Item> where Self: Sized {
        if let Some(last_seg) = self.segments.last() {
            if let &Value::Map(ref map) = &last_seg.data {
                if let &Value::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    if let Some(&Value::Id(id)) = list.last() {
                        return Some(id);
                    }
                }
            }
        }
        return None;
    }
    fn count(self) -> usize where Self: Sized {
        let mut count = 0;
        for seg in self.segments {
            if let &Value::Map(ref map) = &seg.data {
                if let &Value::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    count += list.len();
                }
            }
        }
        return count;
    }
}

