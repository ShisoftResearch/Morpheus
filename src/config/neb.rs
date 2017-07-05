use neb::server::ServerOptions;
use utils::file::slurp;
use serde_yaml;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

pub fn options_from_file<'a>(file: &'a str) -> ServerOptions {
    let file_text = slurp(file).unwrap();
    serde_yaml::from_str(&file_text).unwrap()
}