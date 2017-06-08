pub mod direct;
pub mod indirect;
pub mod hyper;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum EdgeType {
    Direct,
    Indirect,
    Hyper,
    Simple
}
