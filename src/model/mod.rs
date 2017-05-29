use neb::ram::schema::Field;
use neb::ram::types::TypeId;

mod vertex;
mod edge;

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field {
                type_id: TypeId::Id as u32, // point to next list cell
                name: String::from("next"),
                nullable: true,
                is_array: false,
                sub_fields: None,
            },
            Field {
                type_id: TypeId::Id as u32,
                name: String::from("list"),
                nullable: false,
                is_array: true,
                sub_fields: None,
            }
        ];
}