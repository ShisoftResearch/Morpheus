use neb::ram::schema::Field;
use neb::ram::types::Type;

lazy_static! {
    pub static ref EDGE_TEMPLATE: Vec<Field> = vec![
            Field::new(&String::from("_vertices"), Type::Id as u32, false, false, None),
        ];
}