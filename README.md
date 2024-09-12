# rustraft

An implementation of the [extended Raft paper](https://raft.github.io/raft.pdf).

Tests were ported to Rust from the Go tests for [MIT 6.5840 Lab 3: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html). Tests can be run with `cargo test`.

## TODO

[x] Simulate network of Raft nodes for testing
[x] Leader election
[x] Commit log entries
[ ] Persist logs
[ ] Cluster membership changes
[ ] Log compaction / Snapshotting
[ ] Support more interesting properties for simulated network (e.g. delays)
[ ] Support gRPC servers and clients for distributed deployment
[ ] Replace printlns with better logging
[ ] Substitute error handling for panics, expects, and unwraps.
