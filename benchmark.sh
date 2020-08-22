#!/usr/bin/env bash

mvn package

java -jar nitrite-v3/target/benchmarks.jar -rf json -rff results/nitrite-v3.json

java -jar nitrite-v4-mvstore/target/benchmarks.jar -rf json -rff results/nitrite-v4-mvstore.json

java -jar nitrite-v4-rocksdb/target/benchmarks.jar -rf json -rff results/nitrite-v4-rocksdb.json