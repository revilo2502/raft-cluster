package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is a single replicated command.
type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command string `json:"command"` // e.g. "SET key value" or "DEL key"
}

// --- RPC payloads ---

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// ConflictIndex/ConflictTerm speed up log backtracking on mismatch.
	ConflictIndex int `json:"conflictIndex"`
	ConflictTerm  int `json:"conflictTerm"`
}

// Status is a snapshot of node state, used by the HTTP status endpoint / dashboard.
type Status struct {
	ID          string     `json:"id"`
	State       string     `json:"state"`
	Term        int        `json:"term"`
	Leader      string     `json:"leader"`
	VotedFor    string     `json:"votedFor"`
	CommitIndex int        `json:"commitIndex"`
	LastApplied int        `json:"lastApplied"`
	LogLength   int        `json:"logLength"`
	Log         []LogEntry `json:"log"`
	Peers       []string   `json:"peers"`
	Alive       bool       `json:"alive"`
}
