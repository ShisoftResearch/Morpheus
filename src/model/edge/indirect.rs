use neb::ram::schema::Field;
use neb::ram::types::TypeId;
use super::super::ID_LINKED_LIST;

lazy_static! {
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field {
                type_id: TypeId::Id as u32,
                name: String::from("_vertex_a"),
                nullable: false,
                is_array: false,
                sub_fields: None,
            },
            Field {
                type_id: TypeId::Id as u32,
                name: String::from("_vertex_b"),
                nullable: false,
                is_array: false,
                sub_fields: None,
            }
        ];
}