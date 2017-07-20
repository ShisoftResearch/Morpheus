use super::start_server;
use graph;
use graph::*;
use graph::edge::*;
use graph::vertex::*;
use server::schema::{MorpheusSchema, SchemaError};
use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Value, Map};

#[test]
pub fn schemas() {
    let server = start_server(4001);
    let graph = &server.graph;
    let mut edge_schema = MorpheusSchema::new(
        "test_edge_schema",
        None,
        &vec![Field::new(&"test_field".to_string(), TypeId::U32 as u32, false, false, None)]
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
    let mut test_data = Map::new();
    test_data.insert("test_field", Value::U32(1));
    graph.new_vertex(&vertex_schema, test_data.clone()).unwrap();
    graph.new_vertex("test_edge_schema", test_data.clone()).is_err();
}

#[test]
pub fn relationship() {
    let server = start_server(4002);
    let graph = &server.graph;
    //let people_schema = MorpheusSchema::new()
}