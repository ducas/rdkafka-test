# RdKafka.Test

## Why?

Because I like to write test clients to see how things work... Don't you?

## Getting started

Clone... Build... Run

All clients accept an initial broker as their first argument. If ommitted it will default to 192.168.33.50:9092... #justcoz.

### ProducerTest

A simple producer that sends timestamps to a topic named perf-test.

### ConsumerTest

A polling consumer that receives timestamps from a topic named perf-test by joining the perf-group consumer group.

### LatencyTest

A combination of the above 2 that limits to 1000 messages... useful for things like automated performance testing.