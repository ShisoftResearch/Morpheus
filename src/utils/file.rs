use std::fs::File;
use std::io;
use std::io::prelude::*;

pub fn slurp<'a>(file: &'a str) -> io::Result<String> {
    let mut file = File::open(file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    return Ok(contents);
}
