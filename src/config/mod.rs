use crate::{server::MorphesOptions, utils::file::slurp};
use serde_yaml;

pub fn options_from_file<'a>(file: &'a str) -> MorphesOptions {
    let file_text = slurp(file).unwrap();
    let mut config: MorphesOptions = serde_yaml::from_str(&file_text).unwrap();
    config.storage.memory_size *= 1024 * 1024;
    return config;
}
