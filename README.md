# Data Storage Benchmark

This project contains the Data Storage Benchmark, integrated into the HOBBIT Platform.

The Data Storage Benchmark (DSB) represents an RDF continuation of the LDBC Social Network Benchmark. As such, its goal is to evaluate RDF data storage solutions against an interactive workload in a real-world scenario, using various dataset sizes. In order to do so, the benchmark development was choke-point driven. The benchmark tests choke-points identified by the industry as the main bottlenecks of the current generation of storage solutions, e.g. cardinality estimation, choosing correct join order and type, handling scattered index access patterns, parallelism and result reuse, etc. 

The Data Storage Benchmark uses an RDF dataset (DSB Dataset) created by a synthetic data generator (DSB DATAGEN), which mimics an online social network. It is comprised of SPARQL SELECT and SPARQL UPDATE queries combined in query mixes: combinations which mimic real-world behaviour in online social networks. DSB v1.0 uses a sequential query execution.

## Uploading the Benchmark to the HOBBIT Platform

TODO

## Running the Benchmark

TODO