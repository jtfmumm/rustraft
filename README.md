

## TODO

* Rename master to main

* gRPC servers and clients
* Two message types (both broadcast): RequestVote (candidate) and AppendEntries (leader)
* Election timeout (time since last received AppendEntries). Timeout is randomized (50-300ms)
* * If fails, move to candidate state, increment term, and send RequestVote
* * Waits for majority of votes, another leader AppendEntries, or timeout.
* Vote first come first served



