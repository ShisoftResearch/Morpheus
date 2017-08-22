use neb::dovahkiin::expr::symbols::ISYMBOL_MAP;

pub mod crud;

pub fn init_symbols() -> Result<(), ()> {
    ISYMBOL_MAP.insert("insert-cell", crud::cell::Insert {})?;
    ISYMBOL_MAP.insert("insert-vertex", crud::vertex::Insert {})?;

    ISYMBOL_MAP.insert("select-cell", crud::cell::Select {})?;
    ISYMBOL_MAP.insert("select-vertex", crud::vertex::Select {})?;
    ISYMBOL_MAP.insert("select-edge", crud::edge::Select {})?;

    ISYMBOL_MAP.insert("update-cell", crud::cell::Update {})?;
    ISYMBOL_MAP.insert("update-vertex", crud::vertex::Update {})?;
    ISYMBOL_MAP.insert("update-edge", crud::edge::Update {})?;

    ISYMBOL_MAP.insert("delete-cell", crud::cell::Delete {})?;
    ISYMBOL_MAP.insert("delete-vertex", crud::vertex::Delete {})?;
    ISYMBOL_MAP.insert("delete-edge", crud::edge::Delete {})?;
    Ok(())
}