use dovahkiin::types::{Map, OwnedMap, OwnedPrimArray, OwnedValue};
use neb::client::transaction::{Transaction, TxnError};
use neb::ram::cell::{Cell, OwnedCell, MAX_CELL_SIZE};
use neb::ram::schema::Field;
use neb::ram::types::{key_hash, Id, Type};
use std::collections::BTreeSet;

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
    list_map.insert_key_id(
        *LIST_KEY_ID,
        OwnedValue::PrimArray(OwnedPrimArray::Id(vec![])),
    );
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
    let seg_data = &seg.data;
    if let &OwnedValue::Map(ref map) = seg_data {
        let list_key = map.get_by_key_id(*LIST_KEY_ID);
        if let &OwnedValue::PrimArray(ref array) = list_key {
            Ok(array.len())
        } else {
            error!("Count failed, list_key is not array, {:?}", list_key);
            Err(IdListError::FormatError)
        }
    } else {
        error!("Count failed, segment is not map, {:?}", seg_data);
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
            if let OwnedValue::Id(id) = fields[0usize] {
                if !id.is_unit_id() {
                    if let Some(cell) = txn.read(id).await? {
                        if let OwnedValue::Array(ref type_list) = cell[*ID_TYPES_MAP_ID] {
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
                let first_field = &fields[0usize];
                if let OwnedValue::Id(id) = first_field {
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
                            *id
                        }
                    };
                    if type_list_id.is_unit_id() {
                        return Ok(Ok(type_list_id)); // return unit id as not assigned
                    } else {
                        let mut type_list_cell =
                            if let Some(cell) = self.txn.read(type_list_id).await? {
                                cell
                            } else {
                                error!("Cannot find type list with id {:?}", type_list_id);
                                return Ok(Err(IdListError::FormatError));
                            }; // in this time type list should existed
                        if let OwnedValue::Array(ref type_list) = type_list_cell[*ID_TYPES_MAP_ID] {
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
                                    error!(
                                        "Cannot find id type list {:?}, list id {}",
                                        id_list_pair, *ID_TYPES_LIST_ID
                                    );
                                    return Ok(Err(IdListError::FormatError));
                                }
                            }
                        } else {
                            error!(
                                "Id types map is not array {:?}, id {}",
                                type_list_cell.data, *ID_TYPES_MAP_ID
                            );
                            return Ok(Err(IdListError::FormatError));
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
                                &mut type_list_cell[*ID_TYPES_MAP_ID]
                            {
                                type_list.push(OwnedValue::Map(id_list_pair_map));
                            } else {
                                error!(
                                    "id pair is not array {:?}, id {}",
                                    type_list_cell.data, *ID_TYPES_MAP_ID
                                );
                                return Ok(Err(IdListError::FormatError));
                            }
                            self.txn.update(type_list_cell).await?; // update type list               |
                            return Ok(Ok(list_id));
                        } else {
                            return Ok(Ok(Id::unit_id()));
                        }
                    }
                } else {
                    error!(
                        "First field is not id. Got {:?}, cell data {:?}",
                        first_field,
                        fields.data()
                    );
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
                error!(
                    "Last segment cell doesn not existed, id {:?}, root {:?}",
                    last_seg_id, list_root_id
                );
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
        let seg_data = &mut last_seg.data;
        if let &mut OwnedValue::Map(ref mut map) = seg_data {
            let list = map.get_mut_by_key_id(*LIST_KEY_ID);
            if let &mut OwnedValue::PrimArray(OwnedPrimArray::Id(ref mut array)) = list {
                array.push(*id);
            } else {
                error!("Last segment data list is not array {:?}", list);
                return Ok(Err(IdListError::FormatError));
            }
        } else {
            error!("Last segment data is not map {:?}", seg_data);
            return Ok(Err(IdListError::FormatError));
        }
        Ok(Ok(self.txn.update(last_seg).await?))
    }

    pub async fn remove(
        &mut self,
        id: &Id,
        all: bool,
    ) -> Result<Result<(), IdListError>, TxnError> {
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
                        if let &mut OwnedValue::PrimArray(OwnedPrimArray::Id(ref mut array)) =
                            map.get_mut_by_key_id(*LIST_KEY_ID)
                        {
                            let index = match array.iter().position(|v| id == v) {
                                Some(pos) => pos,
                                None => return Ok(Err(IdListError::Unexpected)),
                            };
                            array.remove(index);
                        } else {
                            return Ok(Err(IdListError::FormatError));
                        }
                    } else {
                        return Ok(Err(IdListError::FormatError));
                    }
                    self.txn.update(seg).await.unwrap();
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
            let cell = self
                .txn
                .read_selected(self.next, NEXT_KEY_ID_VEC.clone())
                .await;
            match &cell {
                Ok(Some(fields)) => {
                    let current_id = self.next;
                    if let OwnedValue::Id(ref id) = fields[0usize] {
                        self.next = *id;
                        self.level += 1;
                        return Some(current_id);
                    } else {
                        error!(
                            "Cell does not have next key, got {:?}, key id {}",
                            fields, *NEXT_KEY_ID
                        );
                    }
                }
                _ => {
                    error!("Cannot find next key, got cell {:?}", cell)
                }
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

    pub fn get_curr_seg_list(&self) -> Option<&Vec<Id>> {
        if let Some(ref cell) = self.current_seg {
            if let &OwnedValue::Map(ref map) = &cell.data {
                let list_val = map.get_by_key_id(*LIST_KEY_ID);
                if let &OwnedValue::PrimArray(OwnedPrimArray::Id(ref list)) = list_val {
                    return Some(&list);
                } else {
                    error!("Expecting primitive array got: {:?}", list_val);
                }
            }
        }
        None
    }

    pub async fn next(&mut self) -> Option<Id> {
        loop {
            let mut need_next_seg = false;
            let item = if let Some(list) = self.get_curr_seg_list() {
                let id = list.get(self.current_pos as usize);
                if id.is_none() {
                    need_next_seg = true;
                }
                id
            } else {
                None
            };
            if need_next_seg {
                self.next_seg().await;
                continue;
            } else {
                let res = item.cloned();
                if item.is_some() {
                    self.current_pos += 1;
                }
                return res;
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
                let list_val = map.get_by_key_id(*LIST_KEY_ID);
                if let &OwnedValue::PrimArray(OwnedPrimArray::Id(ref list)) = list_val {
                    if let Some(id) = list.last() {
                        return Some(*id);
                    }
                } else {
                    error!("Expecting primitive array but got {:?}", list_val);
                }
            }
        }
        return last_value;
    }

    pub async fn count(mut self) -> usize
    where
        Self: Sized,
    {
        let mut count = self.get_curr_seg_list().map(|l| l.len()).unwrap_or(0);
        while let Some(seg) = self.segments.next().await {
            if let &OwnedValue::Map(ref map) = &seg.data {
                if let &OwnedValue::PrimArray(OwnedPrimArray::Id(ref list)) =
                    map.get_by_key_id(*LIST_KEY_ID)
                {
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
