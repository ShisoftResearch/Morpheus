use neb::ram::schema::Field;
use neb::ram::types::TypeId;
use super::ID_LINKED_LIST;

lazy_static! {
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field::new(&String::from("_inbound"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("_outbound"), TypeId::Id as u32, true, false, None),
            Field::new(&String::from("_indirected"), TypeId::Id as u32, true, false, None)
        ];
}