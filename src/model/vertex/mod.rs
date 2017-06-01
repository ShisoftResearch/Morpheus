use neb::ram::schema::Field;
use neb::ram::types::TypeId;
use super::ID_LINKED_LIST;

lazy_static! {
    pub static ref VERTEX_TEMPLATE: Vec<Field> = vec![
            Field {
                type_id: TypeId::Id as u32, // point to list cell
                name: String::from("_inbound"),
                nullable: true,
                is_array: false,
                sub_fields: None,
            },
            Field {
                type_id: TypeId::Id as u32,
                name: String::from("_outbound"),
                nullable: true,
                is_array: false,
                sub_fields: None,
            },
            Field {
                type_id: TypeId::Id as u32,
                name: String::from("_indirected"),
                nullable: true,
                is_array: false,
                sub_fields: None,
            },
        ];
}