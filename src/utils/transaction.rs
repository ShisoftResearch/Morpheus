use dovahkiin::types::{OwnedValue, Map};
use neb::client::transaction::{Transaction, TxnError};
use neb::ram::types::Id;

pub async fn set_map_by_key_id(txn: &Transaction, cell_id: Id, key_id: u64, value: OwnedValue)
    -> Result<Option<()>, TxnError> {
    match txn.read(cell_id).await? {
        Some(mut cell) => {
            if let &mut OwnedValue::Map(ref mut map) = &mut cell.data {
                map.insert_key_id(key_id, value);
            } else {
                return Ok(None)
            }
            txn.update(cell).await?;
            return Ok(Some(()))
        },
        None => Ok(None)
    }
}