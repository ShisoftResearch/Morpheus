use neb::ram::schema::Field;
use neb::ram::types::TypeId;

pub mod vertex;
pub mod edge;

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field::new(&String::from("next"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("list"), TypeId::Id as u32, false, true, None)
        ];
}