use super::start_server;
use graph;
use graph::*;
use graph::edge::*;
use graph::vertex::*;
use server::schema::{MorpheusSchema, SchemaError, EMPTY_FIELDS};
use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Value, Map};
use neb::ram::cell::Cell;

#[test]
pub fn schemas() {
    let server = start_server(4001, "schemas");
    let graph = &server.graph;
    let mut edge_schema = MorpheusSchema::new(
        "test_edge_schema",
        None,
        &vec![Field::new(&"test_field", TypeId::U32 as u32, false, false, None)], false
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
    assert_eq!(edge_schema.id, 0);
    graph.new_edge_group(
        &mut edge_schema,
        graph::edge::EdgeAttributes::new(graph::edge::EdgeType::Directed, true)).unwrap();
    let mut vertex_schema = edge_schema.clone();
    vertex_schema.name = "test_vertex_schema".to_string();
    graph.new_vertex_group(&mut vertex_schema).unwrap();
    assert_eq!(edge_schema.id, 1);
    assert_eq!(vertex_schema.id, 2);
    let mut test_data = Map::new();
    test_data.insert("test_field", Value::U32(1));
    graph.new_vertex(&vertex_schema, test_data.clone()).unwrap();
    graph.new_vertex("test_edge_schema", test_data.clone()).is_err();
}

#[test]
pub fn relationship() {
    let server = start_server(4002, "relationship");
    let graph = &server.graph;
    let mut people_schema = MorpheusSchema::new("people", Some(&vec!["name".to_string()]), &vec! [
        Field::new("name", TypeId::String as u32, false, false, None)
    ], true);
    let mut movie_schema = MorpheusSchema::new("movie", Some(&vec!["name".to_string()]), &vec! [
        Field::new("name", TypeId::String as u32, false, false, None),
        Field::new("year", TypeId::U32 as u32, false, false, None)
    ], true);
    let mut acted_in_schema = MorpheusSchema::new("acted-in", None, &vec! [
        Field::new("role", TypeId::String as u32, false, false, None)
    ], true);
    let mut spouse_schema = MorpheusSchema::new("spouse", None, &EMPTY_FIELDS, false);
    graph.new_vertex_group(&mut people_schema).unwrap();
    graph.new_vertex_group(&mut movie_schema).unwrap();
    graph.new_edge_group(
        &mut acted_in_schema,
        EdgeAttributes::new(
            EdgeType::Directed,
            true
        )
    ).unwrap();
    graph.new_edge_group(
        &mut spouse_schema,
        EdgeAttributes::new(
            EdgeType::Undirected,
            false
        )
    ).unwrap();
    assert_eq!(people_schema.id, 1);
    assert_eq!(movie_schema.id, 2);
    assert_eq!(acted_in_schema.id, 3);
    assert_eq!(spouse_schema.id, 4);
    let morgan_freeman = "Morgan Freeman";
    let batman_begins = "Batman Begins";
    let the_dark_knight = "The Dark Knight";
    let the_dark_knight_rises = "The Dark Knight Rises";
    let oblivion = "Oblivion";
    let jeanette = "Jeanette Adair Bradshaw";
    let morgan_vertex = graph.new_vertex("people", data_map!{
        name: morgan_freeman, age: 78 as u8
    }).unwrap();
    graph.new_vertex("movie", data_map!{
        name: batman_begins, year: 2005 as u32
    }).unwrap();
    graph.new_vertex("movie", data_map!{
        name: the_dark_knight, year: 2008 as u32
    }).unwrap();
    graph.new_vertex("movie", data_map!{
        name: the_dark_knight_rises, year: 2012 as u32
    }).unwrap();
    graph.new_vertex("movie", data_map!{
        name: oblivion, year: 2010 as u32
    }).unwrap();
    graph.new_vertex("people", data_map!{
        name: jeanette
    }).unwrap();

    assert_eq!(
        graph.vertex_by_key("people", morgan_freeman)
            .unwrap().unwrap()["name"].String().unwrap(),
        morgan_freeman
    );
    assert_eq!(
        graph.vertex_by_key("movie", batman_begins)
            .unwrap().unwrap()["name"].String().unwrap(),
        batman_begins
    );
    assert_eq!(
        graph.vertex_by_key("movie", the_dark_knight)
            .unwrap().unwrap()["name"].String().unwrap(),
        the_dark_knight
    );
    assert_eq!(
        graph.vertex_by_key("movie", the_dark_knight_rises)
            .unwrap().unwrap()["name"].String().unwrap(),
        the_dark_knight_rises
    );
    assert_eq!(
        graph.vertex_by_key("movie", oblivion)
            .unwrap().unwrap()["name"].String().unwrap(),
        oblivion
    );
    assert_eq!(
        graph.vertex_by_key("people", jeanette)
            .unwrap().unwrap()["name"].String().unwrap(),
        jeanette
    );
    assert_eq!(
        graph.vertex_by_key("people", morgan_freeman)
            .unwrap().unwrap()["age"].U8().unwrap(),
        78u8
    );
}