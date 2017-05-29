use neb::ram::schema::Field;

lazy_static! {
    pub static ref ID_LINKED_LIST: Vec<Field> = vec![
            Field {
                type_id: 6,
                name: String::from("id"),
                nullable:false,
                is_array:false,
                sub_fields: None,
            },
            Field {
                type_id: 20,
                name: String::from("name"),
                nullable:false,
                is_array:false,
                sub_fields: None,
            },
            Field {
                type_id: 10,
                name: String::from("score"),
                nullable:false,
                is_array:false,
                sub_fields: None,
            }
        ];
}