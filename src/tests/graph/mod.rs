use super::start_server;
use crate::graph;
use crate::graph::*;
use crate::graph::edge::*;
use crate::graph::vertex::*;
use crate::server::schema::{MorpheusSchema, SchemaError, EMPTY_FIELDS};
use neb::ram::schema::Field;
use neb::ram::types::{TypeId, Value, Map};
use neb::ram::cell::Cell;
use env_logger;

#[test]
pub fn schemas() {
    let server = start_server(4001, "schemas");
    let graph = &server.graph;
    let edge_schema = MorpheusSchema::new(
        "test_edge_schema",
        None,
        &vec![Field::new(&"test_field", TypeId::U32 as u32, false, false, None)], false
    );
    assert_eq!(edge_schema.id, 0);
    assert!(
    graph.new_edge_group(
        edge_schema.clone(),
        graph::edge::EdgeAttributes::new
            (
                graph::edge::EdgeType::Directed,
                false
            )).wait().is_err());
    let edge_schema_id = graph.new_edge_group(
        edge_schema.clone(),
        graph::edge::EdgeAttributes::new(graph::edge::EdgeType::Directed, true)
    ).wait().unwrap();
    let mut vertex_schema = edge_schema.clone();
    vertex_schema.name = "test_vertex_schema".to_string();
    let vertex_schema_id = graph.new_vertex_group(vertex_schema.clone()).wait().unwrap();
    assert_eq!(edge_schema_id, 1);
    assert_eq!(vertex_schema_id, 2);
    let mut test_data = Map::new();
    vertex_schema.id = vertex_schema_id;
        test_data.insert("test_field", Value::U32(1));
    graph.new_vertex(vertex_schema, test_data.clone()).wait().unwrap();
    graph.new_vertex("test_edge_schema", test_data.clone()).wait().is_err();
}

#[test]
pub fn relationship() {
    env_logger::init();
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
    let people_schema_id = graph.new_vertex_group(people_schema).wait().unwrap();
    let movie_schema_id = graph.new_vertex_group(movie_schema).wait().unwrap();
    let acted_in_schema_id = graph.new_edge_group(
        acted_in_schema,
        EdgeAttributes::new(
            EdgeType::Directed,
            true
        )
    ).wait().unwrap();
    let spouse_schema_id = graph.new_edge_group(
        spouse_schema,
        EdgeAttributes::new(
            EdgeType::Undirected,
            false
        )
    ).wait().unwrap();
    assert_eq!(people_schema_id, 1);
    assert_eq!(movie_schema_id, 2);
    assert_eq!(acted_in_schema_id, 3);
    assert_eq!(spouse_schema_id, 4);
    let morgan_freeman_name = "Morgan Freeman";
    let batman_begins_name = "Batman Begins";
    let the_dark_knight_name = "The Dark Knight";
    let the_dark_knight_rises_name = "The Dark Knight Rises";
    let oblivion_name = "Oblivion";
    let jeanette_name = "Jeanette Adair Bradshaw";
    graph.new_vertex("people", data_map!{
        name: morgan_freeman_name, age: 80 as u8
    }).wait().unwrap();
    graph.new_vertex("movie", data_map!{
        name: batman_begins_name, year: 2005 as u32
    }).wait().unwrap();
    graph.new_vertex("movie", data_map!{
        name: the_dark_knight_name, year: 2008 as u32
    }).wait().unwrap();
    graph.new_vertex("movie", data_map!{
        name: the_dark_knight_rises_name, year: 2012 as u32
    }).wait().unwrap();
    graph.new_vertex("movie", data_map!{
        name: oblivion_name, year: 2010 as u32
    }).wait().unwrap();
    graph.new_vertex("people", data_map!{
        name: jeanette_name
    }).wait().unwrap();

    assert_eq!(
        graph.vertex_by_key("people", morgan_freeman_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        morgan_freeman_name
    );
    assert_eq!(
        graph.vertex_by_key("movie", batman_begins_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        batman_begins_name
    );
    assert_eq!(
        graph.vertex_by_key("movie", the_dark_knight_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        the_dark_knight_name
    );
    assert_eq!(
        graph.vertex_by_key("movie", the_dark_knight_rises_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        the_dark_knight_rises_name
    );
    assert_eq!(
        graph.vertex_by_key("movie", oblivion_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        oblivion_name
    );
    assert_eq!(
        graph.vertex_by_key("people", jeanette_name)
            .wait().unwrap().unwrap()["name"].String().unwrap(),
        jeanette_name
    );
    assert_eq!(
        graph.vertex_by_key("people", morgan_freeman_name)
            .wait().unwrap().unwrap()["age"].U8().unwrap(),
        80u8
    );

    let morgan_freeman =
        graph.vertex_by_key("people", morgan_freeman_name)
            .wait().unwrap().unwrap();

    let batman_begins =
        graph.vertex_by_key("movie", batman_begins_name)
            .wait().unwrap().unwrap();

    let the_dark_knight =
        graph.vertex_by_key("movie", the_dark_knight_name)
            .wait().unwrap().unwrap();

    let the_dark_knight_rises =
        graph.vertex_by_key("movie", the_dark_knight_rises_name)
            .wait().unwrap().unwrap();

    let oblivion =
        graph.vertex_by_key("movie", oblivion_name)
            .wait().unwrap().unwrap();
    let jeanette =
        graph.vertex_by_key("people", jeanette_name)
            .wait().unwrap().unwrap();

    assert_eq!(
        graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 0
    );

    let batman_edge = graph.link(&morgan_freeman, "acted-in", &batman_begins, Some(data_map!{
        role: "Lucius Fox", works_for: "Bruce Wayne"
    })).wait().unwrap().unwrap();
    graph.link(&morgan_freeman, "acted-in", &the_dark_knight, Some(data_map!{
        role: "Lucius Fox"
    })).wait().unwrap().unwrap();
    graph.link(&morgan_freeman, "acted-in", &the_dark_knight_rises, Some(data_map!{
        role: "Lucius Fox"
    })).wait().unwrap().unwrap();

    assert_eq!(
        graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 3);

    let should_error = graph.link(&morgan_freeman, "acted-in", &oblivion, Some(data_map!{
        // missing required field should fail
    })).wait().err().unwrap();
    {
        let neighbourhoods_should_have = 3;
        let morgan_acted_in =
            graph.neighbourhoods::<_, _, String>
            (&morgan_freeman, "acted-in", EdgeDirection::Outbound, &None)
                .wait().unwrap().unwrap();
        if morgan_acted_in.len() != neighbourhoods_should_have {
            panic!("Assertion failed. Wrong neighbourhood number {:?}", &morgan_acted_in);
        }
        assert_eq!(
            graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
                .wait().unwrap().unwrap(),
            neighbourhoods_should_have);
    }

    graph.link(&morgan_freeman, "acted-in", &oblivion, Some(data_map!{
        role: "Beech"
    })).wait().unwrap().unwrap();
    assert_eq!(
        graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 4);
    assert_eq!(
        graph.degree(&morgan_freeman, "spouse", EdgeDirection::Undirected)
            .wait().unwrap().unwrap(), 0);
    graph.link(&morgan_freeman, "spouse", &jeanette, Some(data_map!{
        role: "Sister"
    })).wait().unwrap().err().unwrap();
    assert_eq!(
        graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 4);
    assert_eq!(
        graph.degree(&morgan_freeman, "spouse", EdgeDirection::Undirected)
            .wait().unwrap().unwrap(), 0);
    println!("MF Link Jeanette {:?}", graph.link(&morgan_freeman, "spouse", &jeanette, None).wait());
    assert_eq!( // must use the right edge direction
        graph.degree(&morgan_freeman, "spouse", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 0);
    assert_eq!(
        graph.degree(&morgan_freeman, "acted-in", EdgeDirection::Outbound)
            .wait().unwrap().unwrap(), 4);
    assert_eq!(
        graph.degree(&morgan_freeman, "spouse", EdgeDirection::Undirected)
            .wait().unwrap().unwrap(), 1);
    assert_eq!(
        graph.degree(&jeanette, "spouse", EdgeDirection::Undirected)
            .wait().unwrap().unwrap(), 1);
    println!(
        "Edge sample {:?}",
        graph.neighbourhoods::<_, _, String>
        (&jeanette, "spouse", EdgeDirection::Undirected, &None)
            .wait().unwrap().unwrap());
}

