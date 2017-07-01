use neb::client::transaction::{Transaction, TxnError};
use neb::ram::types::{Value, Id};

pub fn set_map_by_key_id(txn: &mut Transaction, cell_id: &Id, key_id: u64, value: Value)
    -> Result<Option<()>, TxnError> {
    match txn.read(cell_id)? {
        Some(mut cell) => {
            if let &mut Value::Map(ref mut map) = &mut cell.data {
                map.insert_key_id(key_id, value);
            } else {
                return Ok(None)
            }
            txn.update(&cell)?;
            return Ok(Some(()))
        },
        None => Ok(None)
    }
}