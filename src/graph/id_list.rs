use dovahkiin::types::{Map, OwnedMap, OwnedValue, SharedValue, Value};
use futures::{stream, Stream};
use neb::client::transaction::{Transaction, TxnError};
use neb::ram::cell::{Cell, OwnedCell, SharedCell, MAX_CELL_SIZE};
use neb::ram::schema::{Field, Schema};
use neb::ram::types::{key_hash, Id, SharedMap, Type};

use std::collections::BTreeSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::utils::transaction::set_map_by_key_id;

pub const NEXT_KEY: &'static str = "_next";
pub const LIST_KEY: &'static str = "_list";

pub const ID_TYPES_MAP_KEY: &'static str = "_edges";
pub const ID_TYPE_SCHEMA_ID_KEY: &'static str = "_type";
pub const ID_TYPE_ID_LIST_KEY: &'static str = "_type_list";

#[derive(Debug)]
pub enum IdListError {
    ContainerCellNotFound,
    FormatError,
    Unexpected,
}

pub static ID_LIST_SCHEMA_ID: u32 = 100;
pub static TYPE_LIST_SCHEMA_ID: u32 = 150;

lazy_static! {
    pub static ref ID_TYPE_LIST: Field = Field::new(
        "*",
        Type::Map,
        false,
        false,
        Some(vec![Field::new(
            &String::from(ID_TYPES_MAP_KEY),
            Type::Map,
            false,
            true,
            Some(vec![
                Field::new(
                    &String::from(ID_TYPE_SCHEMA_ID_KEY),
                    Type::U32,
                    false,
                    false,
                    None,
                    vec![]
                ),
                Field::new(
                    &String::from(ID_TYPE_ID_LIST_KEY),
                    Type::Id,
                    false,
                    false,
                    None,
                    vec![]
                )
            ]),
            vec![]
        )]),
        vec![]
    );
    pub static ref ID_LINKED_LIST: Field = Field::new(
        "*",
        Type::Map,
        false,
        false,
        Some(vec![
            Field::new(
                &String::from(NEXT_KEY),
                Type::Id,
                false,
                false,
                None,
                vec![]
            ),
            Field::new(&String::from(LIST_KEY), Type::Id, false, true, None, vec![])
        ]),
        vec![]
    );
    pub static ref LIST_CAPACITY: usize =
        (MAX_CELL_SIZE as usize - Type::U32.size().unwrap() - Type::Id.size().unwrap())
            / Type::Id.size().unwrap();
    pub static ref NEXT_KEY_ID: u64 = key_hash(&String::from(NEXT_KEY));
    pub static ref LIST_KEY_ID: u64 = key_hash(&String::from(LIST_KEY));
    pub static ref NEXT_KEY_ID_VEC: Vec<u64> = vec![*NEXT_KEY_ID];
    pub static ref ID_TYPES_MAP_ID: u64 = key_hash(&String::from(ID_TYPES_MAP_KEY));
    pub static ref ID_TYPES_SCHEMA_ID_ID: u64 = key_hash(&String::from(ID_TYPE_SCHEMA_ID_KEY));
    pub static ref ID_TYPES_LIST_ID: u64 = key_hash(&String::from(ID_TYPE_ID_LIST_KEY));
}

pub struct IdList<'a> {
    pub txn: &'a Transaction,
    container_id: Id,
    field_id: u64,
    schema_id: u32,
}

fn empty_list_segment(
    container_id: &Id,
    field_id: u64,
    schema_id: u32,
    level: usize,
) -> (Id, OwnedValue) {
    let str_id = format!(
        "IDLIST-{},{}-{}-{}-{}",
        container_id.higher, container_id.lower, field_id, schema_id, level
    );
    let list_id = Id::new(container_id.higher, key_hash(&str_id));
    let mut list_map = OwnedMap::new();
    list_map.insert_key_id(*NEXT_KEY_ID, OwnedValue::Id(Id::unit_id()));
    list_map.insert_key_id(*LIST_KEY_ID, OwnedValue::Array(Vec::new()));
    return (list_id, OwnedValue::Map(list_map));
}

fn empty_type_list(container_id: &Id, field_id: u64) -> (Id, OwnedValue) {
    let str_id = format!(
        "TYPELIST-{},{}-{}",
        container_id.higher, container_id.lower, field_id
    );
    let list_id = Id::new(container_id.higher, key_hash(&str_id));
    let mut list_map = OwnedMap::new();
    list_map.insert_key_id(*ID_TYPES_MAP_ID, OwnedValue::Array(Vec::new()));
    return (list_id, OwnedValue::Map(list_map));
}

fn count_cell_list(seg: &OwnedCell) -> Result<usize, IdListError> {
    if let &OwnedValue::Map(ref map) = &seg.data {
        if let &OwnedValue::Array(ref array) = map.get_by_key_id(*LIST_KEY_ID) {
            Ok(array.len())
        } else {
            Err(IdListError::FormatError)
        }
    } else {
        Err(IdListError::FormatError)
    }
}

fn val_is_id(val: &OwnedValue, id: &Id) -> bool {
    if let &OwnedValue::Id(ref val_id) = val {
        return val_id != id;
    } else {
        return true;
    }
}

async fn seg_cell_by_id(txn: &Transaction, id: Option<Id>) -> Result<Option<OwnedCell>, TxnError> {
    match id {
        Some(id) => txn.read(id).await,
        None => Ok(None),
    }
}

impl<'a> IdList<'a> {
    pub fn from_txn_and_container(
        txn: &'a Transaction,
        container_id: Id,
        field_id: u64,
        schema_id: u32,
    ) -> IdList<'a> {
        IdList {
            txn: txn,
            container_id,
            field_id,
            schema_id,
        }
    }

    pub async fn cell_types(
        txn: &Transaction,
        container_id: Id,
        field_id: u64,
    ) -> Result<Option<(Id, Vec<u32>)>, TxnError> {
        if let Some(fields) = txn.read_selected(container_id, vec![field_id]).await? {
            if let OwnedValue::Id(id) = fields[0] {
                if !id.is_unit_id() {
                    if let Some(cell) = txn.read(id).await? {
                        if let OwnedValue::Array(ref type_list) = cell.data[*ID_TYPES_MAP_ID] {
                            let mut res = Vec::new();
                            for value in type_list {
                                if let OwnedValue::U32(ref schema_id) =
                                    value[*ID_TYPES_SCHEMA_ID_ID]
                                {
                                    res.push(*schema_id);
                                }
                            }
                            return Ok(Some((id, res)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    async fn get_root_list_id(
        &mut self,
        ensure_container: bool,
    ) -> Result<Result<Id, IdListError>, TxnError> {
        match self
            .txn
            .read_selected(self.container_id, vec![self.field_id])
            .await?
        {
            Some(fields) => {
                if let OwnedValue::Id(id) = fields[0] {
                    let type_list_id = {
                        if id.is_unit_id() && ensure_container {
                            let (type_list_id, type_list) =
                                empty_type_list(&self.container_id, self.field_id);
                            let type_list_cell = OwnedCell::new_with_id(
                                TYPE_LIST_SCHEMA_ID,
                                &type_list_id,
                                type_list,
                            );
                            self.txn.write(type_list_cell).await?;
                            set_map_by_key_id(
                                self.txn,
                                self.container_id,
                                self.field_id,
                                OwnedValue::Id(type_list_id),
                            )
                            .await?;
                            type_list_id
                        } else {
                            id
                        }
                    };
                    if type_list_id.is_unit_id() {
                        return Ok(Ok(type_list_id)); // return unit id as not assigned
                    } else {
                        let mut type_list_cell =
                            if let Some(cell) = self.txn.read(type_list_id).await? {
                                cell
                            } else {
                                return Ok(Err(IdListError::Unexpected));
                            }; // in this time type list should existed
                        if let OwnedValue::Array(ref type_list) =
                            type_list_cell.data[*ID_TYPES_MAP_ID]
                        {
                            if let Some(id_list_pair) = type_list.iter().find(|val| {
                                // trying to find schema list in the type list
                                match val[*ID_TYPES_SCHEMA_ID_ID] {
                                    OwnedValue::U32(schema_id) => schema_id == self.schema_id,
                                    _ => false,
                                }
                            }) {
                                // if found, return it's id
                                if let OwnedValue::Id(list_id) = id_list_pair[*ID_TYPES_LIST_ID] {
                                    return Ok(Ok(list_id));
                                } else {
                                    return Ok(Err(IdListError::Unexpected));
                                }
                            }
                        } else {
                            return Ok(Err(IdListError::Unexpected));
                        }
                        if ensure_container {
                            // if not, create the id list and add it into schema list
                            let (list_id, list_value) = empty_list_segment(
                                &self.container_id,
                                self.field_id,
                                self.schema_id,
                                0,
                            );
                            let list_cell =
                                OwnedCell::new_with_id(ID_LIST_SCHEMA_ID, &list_id, list_value);
                            self.txn.write(list_cell).await?; // create schema id list

                            let mut id_list_pair_map = OwnedMap::new();
                            id_list_pair_map.insert_key_id(
                                *ID_TYPES_SCHEMA_ID_ID,
                                OwnedValue::U32(self.schema_id),
                            );
                            id_list_pair_map
                                .insert_key_id(*ID_TYPES_LIST_ID, OwnedValue::Id(list_id));
                            if let &mut OwnedValue::Array(ref mut type_list) =
                                &mut type_list_cell.data[*ID_TYPES_MAP_ID]
                            {
                                type_list.push(OwnedValue::Map(id_list_pair_map));
                            } else {
                                return Ok(Err(IdListError::Unexpected));
                            }
                            self.txn.update(type_list_cell).await?; // update type list               |
                            return Ok(Ok(list_id));
                        } else {
                            return Ok(Ok(Id::unit_id()));
                        }
                    }
                } else {
                    Ok(Err(IdListError::FormatError))
                }
            }
            None => Ok(Err(IdListError::ContainerCellNotFound)),
        }
    }
    pub async fn iter(&mut self) -> Result<Result<IdListIterator, IdListError>, TxnError> {
        let list_root_id = match self.get_root_list_id(false).await? {
            Err(e) => return Ok(Err(e)),
            Ok(id) => id,
        };
        let mut segments = IdListSegmentIterator::new(self.txn, list_root_id);
        let first_seg = segments.next().await;
        Ok(Ok(IdListIterator {
            segments: segments,
            current_seg: first_seg,
            current_pos: 0,
        }))
    }
    pub async fn all(&mut self) -> Result<Result<Vec<Id>, IdListError>, TxnError> {
        Ok(match self.iter().await?.map(|l| l.collect()) {
            Ok(c) => Ok(c.await),
            Err(e) => Err(e),
        })
    }
    pub async fn count(&mut self) -> Result<Result<usize, IdListError>, TxnError> {
        Ok(match self.iter().await?.map(|l| l.count()) {
            Ok(c) => Ok(c.await),
            Err(e) => Err(e),
        })
    }
    pub async fn add(&mut self, id: &Id) -> Result<Result<(), IdListError>, TxnError> {
        let list_root_id = self.get_root_list_id(true).await?;
        let mut list_level = 0;
        let mut last_seg = {
            // TODO: refill segment under capacity
            let last_seg_id = {
                let mut segments = IdListSegmentIdIterator::new(
                    self.txn,
                    match list_root_id {
                        Ok(v) => v,
                        Err(e) => return Ok(Err(e)),
                    },
                );
                let mut last_seg_id = None;
                while let Some(seg) = segments.next().await {
                    list_level += 1;
                    last_seg_id = Some(seg);
                }
                last_seg_id
            };
            let last_seg = seg_cell_by_id(&mut self.txn, last_seg_id).await?;
            if let Some(seg) = last_seg {
                seg
            } else {
                return Ok(Err(IdListError::Unexpected));
            }
        };
        if match count_cell_list(&mut last_seg) {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        } >= *LIST_CAPACITY
        {
            // create new segment to prevent cell overflow
            list_level += 1;
            let (next_seg_id, next_seg_value) = empty_list_segment(
                &self.container_id,
                self.field_id,
                self.schema_id,
                list_level,
            );
            let next_seg_cell =
                OwnedCell::new_with_id(ID_LIST_SCHEMA_ID, &next_seg_id, next_seg_value);
            self.txn.write(next_seg_cell.clone()).await?;
            set_map_by_key_id(
                &mut self.txn,
                last_seg.id(),
                *NEXT_KEY_ID,
                OwnedValue::Id(next_seg_id),
            )
            .await?;
            last_seg = next_seg_cell;
        }
        if let &mut OwnedValue::Map(ref mut map) = &mut last_seg.data {
            if let &mut OwnedValue::Array(ref mut array) = map.get_mut_by_key_id(*LIST_KEY_ID) {
                array.push(OwnedValue::Id(*id));
            } else {
                return Ok(Err(IdListError::FormatError));
            }
        } else {
            return Ok(Err(IdListError::FormatError));
        }
        Ok(Ok(self.txn.update(last_seg).await?))
    }

    pub async fn remove(
        &mut self,
        id: &Id,
        all: bool,
    ) -> Result<Result<(), IdListError>, TxnError> {
        let id_value = OwnedValue::Id(*id);
        let contained_segs = {
            // collect affected segment cell ids
            let mut iter = match self.iter().await? {
                Ok(v) => v,
                Err(e) => return Ok(Err(e)),
            };
            let mut seg_ids = BTreeSet::new();
            while let Some(iter_id) = iter.next().await {
                if iter_id == *id {
                    if let Some(ref seg) = iter.current_seg {
                        seg_ids.insert(seg.id());
                        if !all {
                            break;
                        }
                    } else {
                        return Ok(Err(IdListError::Unexpected));
                    }
                }
            }
            seg_ids
        };
        for seg_id in contained_segs {
            // mutate cell array
            match self.txn.read(seg_id).await? {
                Some(mut seg) => {
                    if let &mut OwnedValue::Map(ref mut map) = &mut seg.data {
                        if let &mut OwnedValue::Array(ref mut array) =
                            map.get_mut_by_key_id(*LIST_KEY_ID)
                        {
                            if all {
                                array.retain(|v| !val_is_id(v, id));
                            } else {
                                let index = match array.iter().position(|v| val_is_id(v, id)) {
                                    Some(pos) => pos,
                                    None => return Ok(Err(IdListError::Unexpected)),
                                };
                                array.remove(index);
                            }
                        } else {
                            return Ok(Err(IdListError::FormatError));
                        }
                    } else {
                        return Ok(Err(IdListError::FormatError));
                    }
                    self.txn.update(seg);
                    if !all {
                        break;
                    }
                }
                None => return Ok(Err(IdListError::Unexpected)),
            }
        }
        return Ok(Ok(()));
    }

    pub async fn clear_segments(&mut self) -> Result<Result<(), IdListError>, TxnError> {
        let list_root_id = match self.get_root_list_id(true).await? {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        let segments: Vec<_> = IdListSegmentIdIterator::new(self.txn, list_root_id)
            .collect()
            .await;
        for seg_id in segments {
            self.txn.remove(seg_id).await?;
        }
        return Ok(Ok(()));
    }
}

pub struct IdListSegmentIdIterator<'a> {
    pub txn: &'a Transaction,
    next: Id,
    level: u32,
}

impl<'a> IdListSegmentIdIterator<'a> {
    pub fn new(txn: &'a Transaction, head_id: Id) -> IdListSegmentIdIterator<'a> {
        IdListSegmentIdIterator {
            txn: txn,
            next: head_id,
            level: 1,
        }
    }

    pub async fn next(&mut self) -> Option<Id> {
        if !self.next.is_unit_id() {
            match self
                .txn
                .read_selected(self.next, NEXT_KEY_ID_VEC.clone())
                .await
            {
                Ok(Some(fields)) => {
                    let current_id = self.next;
                    if let OwnedValue::Id(ref id) = fields[0] {
                        self.next = *id;
                        self.level += 1;
                        return Some(current_id);
                    }
                }
                _ => {}
            }
        }
        None
    }

    pub async fn collect(mut self) -> Vec<Id> {
        let mut res = vec![];
        while let Some(id) = self.next().await {
            res.push(id);
        }
        res
    }
}

pub struct IdListSegmentIterator<'a> {
    pub id_iter: IdListSegmentIdIterator<'a>,
}

impl<'a> IdListSegmentIterator<'a> {
    pub fn new(txn: &'a Transaction, head_id: Id) -> IdListSegmentIterator<'a> {
        IdListSegmentIterator {
            id_iter: IdListSegmentIdIterator::new(txn, head_id),
        }
    }

    pub async fn next(&mut self) -> Option<OwnedCell> {
        let next_id = self.id_iter.next().await;
        if let Ok(Some(cell)) = seg_cell_by_id(self.id_iter.txn, next_id).await {
            Some(cell)
        } else {
            None
        }
    }

    pub async fn last(mut self) -> Option<OwnedCell> {
        let mut res = self.next().await;
        let mut last = res.clone();
        while res.is_some() {
            last = res;
            res = self.next().await;
        }
        last
    }
}

pub struct IdListIterator<'a> {
    pub segments: IdListSegmentIterator<'a>,
    current_seg: Option<OwnedCell>,
    current_pos: u32,
}

impl<'a> IdListIterator<'a> {
    pub async fn next_seg(&mut self) {
        self.current_seg = self.segments.next().await;
        self.current_pos = 0;
    }

    pub fn get_curr_seg_list(&self) -> Option<&Vec<OwnedValue>> {
        if let Some(ref cell) = self.current_seg {
            let pos = self.current_pos;
            if let &OwnedValue::Map(ref map) = &cell.data {
                if let &OwnedValue::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    return Some(&list);
                }
            }
        }
        None
    }

    pub async fn next(&mut self) -> Option<Id> {
        loop {
            let mut need_next_seg = false;
            let item = if let Some(list) = self.get_curr_seg_list() {
                if let Some(&OwnedValue::Id(id)) = list.get(self.current_pos as usize) {
                    Some(id)
                } else {
                    need_next_seg = true;
                    None
                }
            } else {
                None
            };
            if need_next_seg {
                self.next_seg().await;
                continue;
            } else {
                if item.is_some() {
                    self.current_pos += 1;
                }
                return item;
            }
        }
    }

    pub async fn last(self) -> Option<Id>
    where
        Self: Sized,
    {
        let last_value = self
            .get_curr_seg_list()
            .map(|l| l.last())
            .unwrap_or(None)
            .cloned();
        if let Some(last_seg) = self.segments.last().await {
            if let &OwnedValue::Map(ref map) = &last_seg.data {
                if let &OwnedValue::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    if let Some(&OwnedValue::Id(id)) = list.last() {
                        return Some(id);
                    }
                }
            }
        }
        return if let Some(OwnedValue::Id(id)) = last_value {
            Some(id)
        } else {
            None
        };
    }

    pub async fn count(mut self) -> usize
    where
        Self: Sized,
    {
        let mut count = self.get_curr_seg_list().map(|l| l.len()).unwrap_or(0);
        while let Some(seg) = self.segments.next().await {
            if let &OwnedValue::Map(ref map) = &seg.data {
                if let &OwnedValue::Array(ref list) = map.get_by_key_id(*LIST_KEY_ID) {
                    count += list.len();
                }
            }
        }
        return count;
    }

    pub async fn collect(mut self) -> Vec<Id> {
        let mut res = vec![];
        while let Some(id) = self.next().await {
            res.push(id);
        }
        res
    }
}
