#!/usr/bin/env bash

while true
do
cargo test --color=always --package morpheus --bin morpheus relationship -- --nocapture
done