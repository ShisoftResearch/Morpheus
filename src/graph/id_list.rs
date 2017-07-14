use neb::ram::schema::Field;
use neb::ram::cell::{MAX_CELL_SIZE, Cell};
use neb::ram::types::{TypeId, Id, Map, Value, id_io, u32_io, key_hash};
use neb::client::transaction::{Transaction, TxnError};

use std::collections::BTreeSet;

use utils::transaction::set_map_by_key_id;

pub const NEXT_KEY: &'static str = "_next";
pub const LIST_KEY: &'static str = "_list";

pub const ID_TYPES_MAP_KEY: &'static str = "_edges";
pub const ID_TYPE_SCHEMA_ID_KEY: &'static str = "_type";
pub const ID_TYPE_ID_LIST_KEY: &'static str = "_list";

pub enum IdListError {
    ContainerCellNotFound,
    FormatError,
    Unexpected
}

pub static ID_LIST_SCHEMA_ID: u32 = 100;
pub static TYPE_LIST_SCHEMA_ID: u32 = 150;

lazy_static! {
    pub static ref ID_TYPE_LIST: Vec<Field> = vec![
        Field::new(&String::from(ID_TYPES_MAP_KEY), TypeId::Map as u32, false, true,
            Some(vec![
                Field::new(&String::from(ID_TYPE_SCHEMA_ID_KEY), TypeId::U32 as u32, false, false, None),
                Field::new(&String::from(ID_TYPE_ID_LIST_KEY), TypeId::Id as u32, false, true, None)
            ]))
    ];
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
        Field::new(&String::from(NEXT_KEY), TypeId::Id as u32, false, false, None),
        Field::new(&String::from(LIST_KEY), TypeId::Id as u32, false, true, None)
    ];
    pub static ref LIST_CAPACITY: usize =
        ((MAX_CELL_SIZE - u32_io::size(0) - id_io::size(0)) / id_io::size(0));
    pub static ref NEXT_KEY_ID: u64 = key_hash(&String::from(NEXT_KEY));
    pub static ref LIST_KEY_ID: u64 = key_hash(&String::from(LIST_KEY));
    pub static ref NEXT_KEY_ID_VEC: Vec<u64> = vec![*NEXT_KEY_ID];

    pub static ref ID_TYPES_MAP_ID: u64 = key_hash(&String::from(ID_TYPES_MAP_KEY));
    pub static ref ID_TYPES_SCHEMA_ID_ID: u64 = key_hash(&String::from(ID_TYPE_SCHEMA_ID_KEY));
    pub static ref ID_TYPES_LIST_ID: u64 = key_hash(&String::from(ID_TYPE_ID_LIST_KEY));
}

pub struct IdList<'a> {
    txn: &'a mut Transaction,
    container_id: Id,
    field_id: u64,
    schema_id: u32
}

fn empty_list_segment(container_id: &Id, field_id: u64, schema_id: u32, level: usize) -> (Id, Value) {
    let str_id = format!("IDLIST-{},{}-{}-{}-{}", container_id.higher, container_id.lower, field_id, schema_id, level);
    let list_id = Id::new(container_id.higher, key_hash(&str_id));
    let mut list_map = Map::new();
    list_map.insert_key_id(*NEXT_KEY_ID, Value::Id(Id::unit_id()));
    list_map.insert_key_id(*LIST_KEY_ID, Value::Array(Vec::<Value>::new()));
    return (list_id, Value::Map(list_map));
}

fn empty_type_list(container_id: &Id, field_id: u64) -> (Id, Value) {
    let str_id = format!("TYPELIST-{},{}-{}", container_id.higher, container_id.lower, field_id);
    let list_id = Id::new(container_id.higher, key_hash(&str_id));
    let mut list_map = Map::new();
    list_map.insert_key_id(*ID_TYPES_MAP_ID, Value::Array(Vec::<Value>::new()));
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

fn val_is_id(val: &Value, id: &Id) -> bool {
    if let &Value::Id(ref val_id) = val {
        return val_id != id;
    } else {
        return true;
    }
}

fn seg_cell_by_id(txn: &mut Transaction, id: Option<Id>) -> Result<Option<Cell>, TxnError> {
    match id {
        Some(id) => txn.read(&id),
        None => Ok(None)
    }
}

impl<'a> IdList <'a> {
    pub fn from_txn_and_container(txn: &'a mut Transaction, container_id: &Id, field_id: u64, schema_id: u32) -> IdList<'a> {
        IdList {
            txn: txn,
            container_id: *container_id,
            field_id: field_id,
            schema_id: schema_id
        }
    }
    fn get_root_list_id(&mut self, ensure_container: bool) -> Result<Result<Id, IdListError>, TxnError> {
        match self.txn.read_selected(&self.container_id, &vec![self.field_id])? {
            Some(fields) => {
                if let Some(&Value::Id(id)) = fields.get(0) {
                    let type_list_id = {
                        if id.is_unit_id() && ensure_container {
                            let (type_list_id, type_list) = empty_type_list(&self.container_id, self.field_id);
                            let type_list_cell = Cell::new_with_id(TYPE_LIST_SCHEMA_ID, &type_list_id, type_list);
                            self.txn.write(&type_list_cell)?;
                            set_map_by_key_id(self.txn, &self.container_id, self.field_id, Value::Id(type_list_id))?;
                            type_list_id
                        } else {id}
                    };
                    if type_list_id.is_unit_id() {
                        return Ok(Ok(type_list_id)); // return unit id as not assigned
                    } else {
                        let mut type_list_cell = if let Some(cell) = self.txn.read(&type_list_id)?
                            { cell } else { return Ok(Err(IdListError::Unexpected)); }; // in this time type list should existed
                        if let Value::Array(ref type_list) = type_list_cell.data[*ID_TYPES_MAP_ID] {
                            if let Some(id_list_pair) = type_list.iter().find(|val| { // trying to find schema list in the type list
                                match val[*ID_TYPES_SCHEMA_ID_ID] {
                                    Value::U32(schema_id) => schema_id == self.schema_id,
                                    _ => false
                                }
                            }) { // if found, return it's id
                                if let Value::Id(list_id) = id_list_pair[*ID_TYPES_LIST_ID] {
                                    return Ok(Ok(list_id));
                                } else {
                                    return Ok(Err(IdListError::Unexpected))
                                }
                            }
                        } else { return Ok(Err(IdListError::Unexpected)); }
                        if ensure_container {
                            // if not, create the id list and add it into schema list
                            let (list_id, list_value) = empty_list_segment(&self.container_id, self.field_id, self.schema_id, 0);
                            let list_cell = Cell::new_with_id(ID_LIST_SCHEMA_ID, &list_id, list_value);
                            self.txn.write(&list_cell)?; // create schema id list

                            let mut id_list_pair_map = Map::new();
                            id_list_pair_map.insert_key_id(*ID_TYPES_SCHEMA_ID_ID, Value::U32(self.schema_id));
                            id_list_pair_map.insert_key_id(*ID_TYPES_LIST_ID, Value::Id(list_id));
                            if let &mut Value::Array(ref mut type_list) = &mut type_list_cell.data[*ID_TYPES_MAP_ID] {
                                type_list.push(Value::Map(id_list_pair_map));
                            } else { return Ok(Err(IdListError::Unexpected)); }
                            self.txn.update(&type_list_cell)?; // update type list               |
                            return Ok(Ok(list_id));
                        } else {
                            return Ok(Ok(Id::unit_id()));
                        }
                    }
                } else {
                    Ok(Err(IdListError::FormatError))
                }
            },
            None => {
                Ok(Err(IdListError::ContainerCellNotFound))
            }
        }
    }
    pub fn iter(&mut self) -> Result<Result<IdListIterator, IdListError>, TxnError> {
        let list_root_id = match self.get_root_list_id(false)? {
            Err(e) => return Ok(Err(e)), Ok(id) => id
        };
        let mut segments = IdListSegmentIterator::new(&mut self.txn, list_root_id);
        let current_seg = segments.next();
        Ok(Ok(IdListIterator {
            segments: segments,
            current_seg: current_seg,
            current_pos: 0,
        }))
    }
    pub fn all(&mut self)
               -> Result<Result<Vec<Id>, IdListError>, TxnError> {
        Ok(self.iter()?.map(|l| l.collect()))
    }
    pub fn count(&mut self)
                 -> Result<Result<usize, IdListError>, TxnError> {
        Ok(self.iter()?.map(|l| l.count()))
    }
    pub fn add(&mut self, id: &Id) -> Result<Result<(), IdListError>, TxnError> {
        let list_root_id = self.get_root_list_id(true)?;
        let mut list_level = 0;
        let mut last_seg = {
            let last_seg_id = {
                let mut segments = IdListSegmentIdIterator::new(
                    &mut self.txn,
                    match list_root_id {
                        Ok(v) => v, Err(e) => return Ok(Err(e))
                    });
                let mut last_seg_id = None;
                for seg in segments {
                    list_level += 1;
                    last_seg_id = Some(seg);
                }
                last_seg_id
            };
            let last_seg = seg_cell_by_id(&mut self.txn, last_seg_id)?;
            if let Some(seg) = last_seg { seg } else { return Ok(Err(IdListError::Unexpected)); }
        };
        if match count_cell_list(&mut last_seg) {
            Ok(c) => c, Err(e) => return Ok(Err(e))
        } >= *LIST_CAPACITY { // create new segment to prevent cell overflow
            list_level += 1;
            let (next_seg_id, next_seg_value) = empty_list_segment(&self.container_id, self.field_id, self.schema_id, list_level);
            let next_seg_cell = Cell::new_with_id(ID_LIST_SCHEMA_ID, &next_seg_id, next_seg_value);
            self.txn.write(&next_seg_cell)?;
            set_map_by_key_id(&mut self.txn, &last_seg.id(), *NEXT_KEY_ID, Value::Id(next_seg_id))?;
            last_seg = next_seg_cell;
        }
        if let &mut Value::Map(ref mut map) = &mut last_seg.data {
            if let &mut Value::Array(ref mut array) = map.get_mut_by_key_id(*LIST_KEY_ID) {
                array.push(Value::Id(*id));
            } else {
                return Ok(Err(IdListError::FormatError));
            }
        } else {
            return Ok(Err(IdListError::FormatError));
        }
        Ok(Ok(self.txn.update(&last_seg)?))
    }
    pub fn remove(&mut self, id: &Id, all: bool) -> Result<Result<(), IdListError>, TxnError> {
        let id_value = Value::Id(*id);
        let mut contained_segs = { // collect affected segment cell ids
            let mut iter = match self.iter()? {
                Ok(v) => v, Err(e) => return Ok(Err(e))
            };
            let mut seg_ids = BTreeSet::new();
            while let Some(iter_id) = iter.next() {
                if iter_id == *id {
                    if let Some(ref seg) = iter.current_seg {
                        seg_ids.insert(seg.id());
                    } else {
                        return Ok(Err(IdListError::Unexpected));
                    }
                }
            }
            seg_ids
        };
        for seg_id in &contained_segs { // mutate cell array
            match self.txn.read(seg_id)? {
                Some(mut seg) => {
                    if let &mut Value::Map(ref mut map) = &mut seg.data {
                        if let &mut Value::Array(ref mut array) = map.get_mut_by_key_id(*LIST_KEY_ID) {
                            if all {
                                array.retain(|v| { !val_is_id(v, id) });
                            } else {
                                let index = match array.iter().position(|v| { val_is_id(v, id) }) {
                                    Some(pos) => pos, None => return Ok(Err(IdListError::Unexpected))
                                };
                                array.remove(index);
                            }
                        } else {
                            return Ok(Err(IdListError::FormatError));
                        }
                    } else {
                        return Ok(Err(IdListError::FormatError));
                    }
                    self.txn.update(&seg);
                    if !all { break; }
                },
                None => return Ok(Err(IdListError::Unexpected))
            }
        }
        return Ok(Ok(()));
    }
    pub fn clear(&mut self) -> Result<Result<(), IdListError>, TxnError> {
        let list_root_id = match self.get_root_list_id(true)? {
            Ok(v) => v, Err(e) => return Ok(Err(e))
        };
        let segments: Vec<_> = IdListSegmentIdIterator::new(&mut self.txn, list_root_id).collect();
        for seg_id in segments {
            self.txn.remove(&seg_id)?;
        }
        set_map_by_key_id(self.txn, &self.container_id, self.field_id, Value::Id(Id::unit_id()));
        return Ok(Ok(()))
    }
}

pub struct IdListSegmentIdIterator<'a> {
    txn: &'a mut Transaction,
    next: Id,
    level: u32
}

impl <'a> IdListSegmentIdIterator<'a> {
    pub fn new(txn: &'a mut Transaction, head_id: Id) -> IdListSegmentIdIterator<'a> {
        IdListSegmentIdIterator {
            txn: txn,
            next: head_id,
            level: 1
        }
    }
}

impl <'a> Iterator for IdListSegmentIdIterator<'a> {

    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.next.is_unit_id() {
            match self.txn.read_selected(&self.next, &*NEXT_KEY_ID_VEC) {
                Ok(Some(fields)) => {
                    let current_id = self.next;
                    if let Some(&Value::Id(ref id)) = fields.get(0) {
                        self.next = *id;
                        self.level += 1;
                        return Some(current_id);
                    }
                },
                _ => {}
            }
        }
        None
    }
}

pub struct IdListSegmentIterator<'a> {
    id_iter: IdListSegmentIdIterator<'a>
}

impl <'a>IdListSegmentIterator<'a> {
    pub fn new(txn: &'a mut Transaction, head_id: Id) -> IdListSegmentIterator<'a> {
        IdListSegmentIterator {
            id_iter: IdListSegmentIdIterator::new(txn, head_id)
        }
    }
}

impl <'a> Iterator for IdListSegmentIterator <'a> {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        let next_id = self.id_iter.next();
        if let Ok(Some(cell)) = seg_cell_by_id(self.id_iter.txn, next_id) {
            Some(cell)
        } else {
            None
        }
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
