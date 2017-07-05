use neb::server::*;
use utils::file::slurp;
use serde_yaml;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

pub fn options_from_file<'a>(file: &'a str) -> ServerOptions {
    let file_text = slurp(file).unwrap();
    let mut config: ServerOptions = serde_yaml::from_str(&file_text).unwrap();
    config.memory_size *= 1024 * 1024;
    return config;
}