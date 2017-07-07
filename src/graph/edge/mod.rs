pub mod direct;
pub mod indirect;
pub mod hyper;

use neb::ram::cell::Cell;
use neb::ram::types::Id;
use graph::vertex::Vertex;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum EdgeType {
    Direct,
    Indirect,
    Hyper,
    Simple
}

pub trait Edge {
    fn edge_type() -> EdgeType;
    fn field_a() -> u64;
    fn field_b() -> u64;
    fn opposite(field_id: u64) -> Vec<Vertex>;
    fn require_edge_cell() -> bool;
    fn from_id<E>(id: Id) -> E where E: Edge;
    fn edge_cell(&self) -> Option<&Cell>;
    fn delete_edge(&self) -> Result<(), ()>;
}