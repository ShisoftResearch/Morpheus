use neb::ram::schema::Field;
use neb::ram::cell::{MAX_CELL_SIZE, Cell};
use neb::ram::types::{TypeId, Id, Map, Value, id_io, u32_io, key_hash};
use neb::client::transaction::{Transaction, TxnError};

use graph::vertex::Vertex;
use utils::transaction::set_map_by_key_id;

pub const NEXT_KEY: &'static str = "_next";
pub const LIST_KEY: &'static str = "_list";

pub enum IdListError {
    VertexNotFound,
    FormatError,
    TxnError(TxnError)
}

pub static ID_LIST_SCHEMA_ID: u32 = 100;

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
    vertex_id: Id,
    field_id: u64
}

fn empty_list_segment(vertex_id: &Id, level: usize) -> (Id, Value) {
    let str_id = format!("IDLIST-{},{}-{}", vertex_id.higher, vertex_id.lower, 1);
    let list_id = Id::new(vertex_id.higher, key_hash(&str_id));
    let mut list_map = Map::new();
    list_map.insert_key_id(*NEXT_KEY_ID, Value::Id(Id::unit_id()));
    list_map.insert_key_id(*LIST_KEY_ID, Value::Array(Vec::<Value>::new()));
    return (list_id, Value::Map(list_map));
}

fn count_cell_list(seg: &Cell) -> Result<usize, IdListError> {
    if let &Value::Map(ref map) = &seg.data {
        if let &Value::Array(ref array) = map.get_by_key_id(*LIST_KEY_ID) {
            Ok(array.len())
        } else {
            Err(IdListError::FormatError)
        }
    } else {
        Err(IdListError::FormatError)
    }
}

impl<'a> IdList <'a> {
    pub fn from_txn_vertex(txn: &'a mut Transaction, vertex_id: &Id, field_id: u64) -> IdList<'a> {
        IdList {
            txn: txn,
            vertex_id: *vertex_id,
            field_id: field_id,
        }
    }
    fn get_root_list_id(&mut self, ensure_vertex: bool) -> Result<Id, IdListError> {
        match self.txn.read_selected(&self.vertex_id, &vec![self.field_id]) {
            Err(e) => Err(IdListError::TxnError(e)),
            Ok(Some(fields)) => {
                if let Some(&Value::Id(id)) = fields.get(0) {
                    Ok(id)
                } else {
                    Err(IdListError::FormatError)
                }
            },
            Ok(None) => {
                if ensure_vertex {
                    let (list_id, list_value) = empty_list_segment(&self.vertex_id, 0);
                    let list_cell = Cell::new_with_id(ID_LIST_SCHEMA_ID, &list_id, list_value);
                    match self.txn.write(&list_cell) {
                        Ok(()) => {},
                        Err(e) => return Err(IdListError::TxnError(e))
                    }
                    set_map_by_key_id(self.txn, &self.vertex_id, self.field_id, Value::Id(list_id));
                    Ok(list_id)
                } else {
                    Err(IdListError::VertexNotFound)
                }
            }
        }
    }
    pub fn iter(&mut self) -> Result<IdListIterator, IdListError> {
        let list_root_id = self.get_root_list_id(false)?;
        let mut segments = IdListSegmentIterator::new(&mut self.txn, list_root_id);
        let first_cell = segments.next();
        Ok(IdListIterator {
            segments: segments,
            current_seg: first_cell,
            current_pos: 0,
        })
    }
    pub fn all(&mut self) -> Result<Vec<Id>, IdListError> {
        Ok(self.iter()?.collect())
    }
    pub fn count(&mut self) -> Result<usize, IdListError> {
        Ok(self.iter()?.count())
    }
    pub fn add(&mut self, id: Id) -> Result<(), IdListError> {
        let list_root_id = self.get_root_list_id(true)?;
        let mut last_seg = {
            let mut segments = IdListSegmentIterator::new(&mut self.txn, list_root_id);
            if let Some(cell) = segments.last() { cell } else {
                return Err(IdListError::FormatError);
            }
        };
        if count_cell_list(&mut last_seg)? >= *LIST_CAPACITY { // create new segment to prevent cell overflow
            let list_level = IdListSegmentIterator::new(&mut self.txn, list_root_id).count();
            let (next_seg_id, next_seg_value) = empty_list_segment(&self.vertex_id, list_level);
            let next_seg_cell = Cell::new_with_id(ID_LIST_SCHEMA_ID, &next_seg_id, next_seg_value);
            match self.txn.write(&next_seg_cell) {
                Ok(()) => {},
                Err(e) => return Err(IdListError::TxnError(e))
            }
            set_map_by_key_id(&mut self.txn, &last_seg.id(), *NEXT_KEY_ID, Value::Id(next_seg_id));
            last_seg = next_seg_cell;
        }
        if let &mut Value::Map(ref mut map) = &mut last_seg.data {
            if let Some(&mut Value::Array(ref mut array)) = map.get_mut_by_key_id(*LIST_KEY_ID) {
                array.push(Value::Id(id));
            } else {
                return Err(IdListError::FormatError);
            }
        } else {
            return Err(IdListError::FormatError);
        }
        match self.txn.update(&last_seg) {
            Ok(()) => Ok(()),
            Err(e) => Err(IdListError::TxnError(e))
        }
    }
}

pub struct IdListSegmentIterator<'a> {
    txn: &'a mut Transaction,
    next: Id,
    level: u32
}

impl <'a> IdListSegmentIterator<'a> {
    pub fn new(txn: &'a mut Transaction, head_id: Id) -> IdListSegmentIterator<'a> {
        IdListSegmentIterator {
            txn: txn,
            next: head_id,
            level: 1
        }
    }
}

impl <'a> Iterator for IdListSegmentIterator<'a> {

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
                        self.level += 1;
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
    segments: IdListSegmentIterator<'a>,
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

