#!/usr/bin/env bash

mvn package

mkdir reports

java -jar nitrite-v3/target/benchmarks.jar -rf json -rff reports/nitrite-v3.json

java -jar nitrite-v4/target/benchmarks.jar -rf json -rff reports/nitrite-v4.json