use super::start_server;
use graph;
use server::schema::{MorpheusSchema, SchemaError};
use neb::ram::schema::Field;
use neb::ram::types::TypeId;

#[test]
pub fn schemas() {
    let server = start_server(4001);
    let graph = &server.graph;
    let mut edge_schema = MorpheusSchema::new(
        "test_edge_schema",
        None,
        &vec![Field::new(&"test_field".to_string(), TypeId::String as u32, false, false, None)]
    );
    assert_eq!(edge_schema.id, 0);
    assert!(
    graph.new_edge_group(
        &mut edge_schema,
        graph::edge::EdgeAttributes::new
            (
                graph::edge::EdgeType::Directed,
                false
            )).is_err());
    graph.new_edge_group(
        &mut edge_schema,
        graph::edge::EdgeAttributes::new(graph::edge::EdgeType::Directed, true)).unwrap();
    let mut vertex_schema = edge_schema.clone();
    vertex_schema.name = "test_vertex_schema".to_string();
    graph.new_vertex_group(&mut vertex_schema).unwrap();
    assert!(edge_schema.id > 0);
    assert!(vertex_schema.id > 0);
}