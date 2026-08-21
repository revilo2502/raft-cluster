package raft

// Transport abstracts how a node talks to its peers, so the core Raft
// logic doesn't care whether it's HTTP, gRPC, or in-memory (for tests).
type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// ApplyFunc is called once a log entry is committed, so the KV store
// (or any state machine) can apply it.
type ApplyFunc func(entry LogEntry)
