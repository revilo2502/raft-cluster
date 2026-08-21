package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	minElectionTimeoutMs = 300
	maxElectionTimeoutMs = 600
	heartbeatIntervalMs  = 100
)

type Raft struct {
	mu sync.Mutex

	id    string
	peers []string // all peer IDs (not including self)

	transport Transport
	apply     ApplyFunc

	// Persistent state (would be flushed to disk in Phase 4)
	currentTerm int
	votedFor    string
	log         []LogEntry // 1-indexed conceptually; log[0] is a dummy sentinel

	// Volatile state
	state       State
	leaderID    string
	commitIndex int
	lastApplied int

	// Leader-only volatile state
	nextIndex  map[string]int
	matchIndex map[string]int

	electionResetAt time.Time
	stopCh          chan struct{}
	alive           bool
}

func NewRaft(id string, peers []string, transport Transport, apply ApplyFunc) *Raft {
	r := &Raft{
		id:          id,
		peers:       peers,
		transport:   transport,
		apply:       apply,
		state:       Follower,
		log:         []LogEntry{{Term: 0, Index: 0, Command: ""}}, // sentinel
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		stopCh:      make(chan struct{}),
		alive:       true,
	}
	return r
}

func (r *Raft) Start() {
	go r.electionTimerLoop()
}

func (r *Raft) Stop() {
	r.mu.Lock()
	r.alive = false
	r.mu.Unlock()
	close(r.stopCh)
}

// SetAlive simulates the node being cut off from the network in both
// directions (like a real partition/crash), not just refusing inbound
// requests. Used by the chaos-testing /admin/kill and /admin/revive endpoints.
func (r *Raft) SetAlive(alive bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alive = alive
	if alive {
		// Rejoin as a follower and reset the timer so it doesn't
		// instantly re-trigger an election with a stale term view.
		r.state = Follower
		r.resetElectionTimer()
	}
}

func (r *Raft) IsAlive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.alive
}

func randomElectionTimeout() time.Duration {
	ms := minElectionTimeoutMs + rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs)
	return time.Duration(ms) * time.Millisecond
}

// electionTimerLoop is the heartbeat of the whole algorithm: if a follower
// doesn't hear from a leader within the timeout, it starts an election.
func (r *Raft) electionTimerLoop() {
	timeout := randomElectionTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			if !r.alive {
				r.mu.Unlock()
				continue
			}
			elapsed := time.Since(r.electionResetAt)
			state := r.state
			r.mu.Unlock()

			if state != Leader && elapsed >= timeout {
				r.startElection()
				timeout = randomElectionTimeout()
			}
		}
	}
}

func (r *Raft) resetElectionTimer() {
	r.electionResetAt = time.Now()
}

// startElection turns this node into a candidate and requests votes from peers.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	term := r.currentTerm
	r.votedFor = r.id
	r.resetElectionTimer()
	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term
	r.mu.Unlock()

	log.Printf("[%s] starting election for term %d", r.id, term)

	votes := 1 // vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := r.transport.SendRequestVote(peer, args)
			if err != nil || reply == nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				return
			}
			if r.state != Candidate || r.currentTerm != term {
				return // stale response, we've moved on
			}
			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				v := votes
				voteMu.Unlock()
				if v > (len(r.peers)+1)/2 {
					r.becomeLeaderLocked()
				}
			}
		}(peer)
	}
	wg.Wait()
}

// becomeFollower must be called with the lock held.
func (r *Raft) becomeFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderID = ""
	r.resetElectionTimer()
}

// becomeLeaderLocked must be called with the lock held.
func (r *Raft) becomeLeaderLocked() {
	if r.state == Leader {
		return
	}
	r.state = Leader
	r.leaderID = r.id
	log.Printf("[%s] became LEADER for term %d", r.id, r.currentTerm)

	lastIndex := len(r.log) - 1
	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIndex + 1
		r.matchIndex[peer] = 0
	}
	go r.leaderLoop(r.currentTerm)
}

// leaderLoop periodically sends AppendEntries (heartbeats + log replication)
// to all peers while this node remains leader for the given term.
func (r *Raft) leaderLoop(term int) {
	ticker := time.NewTicker(heartbeatIntervalMs * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			if !r.alive {
				r.mu.Unlock()
				continue
			}
			if r.state != Leader || r.currentTerm != term {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			r.broadcastAppendEntries(term)
		}
	}
}

func (r *Raft) broadcastAppendEntries(term int) {
	var wg sync.WaitGroup
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			r.sendAppendEntriesToPeer(peer, term)
		}(peer)
	}
	wg.Wait()

	// After replicating, check if we can advance commitIndex.
	r.mu.Lock()
	if r.state == Leader && r.currentTerm == term {
		r.maybeAdvanceCommitIndexLocked()
	}
	r.mu.Unlock()
}

func (r *Raft) sendAppendEntriesToPeer(peer string, term int) {
	r.mu.Lock()
	if !r.alive || r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevLogIndex := ni - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(r.log) {
		prevLogTerm = r.log[prevLogIndex].Term
	}
	var entries []LogEntry
	if ni < len(r.log) {
		entries = append(entries, r.log[ni:]...)
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.transport.SendAppendEntries(peer, args)
	if err != nil || reply == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}

	if reply.Success {
		r.matchIndex[peer] = prevLogIndex + len(entries)
		r.nextIndex[peer] = r.matchIndex[peer] + 1
	} else {
		// Back off using the conflict hint, falling back to a simple decrement.
		if reply.ConflictIndex > 0 {
			r.nextIndex[peer] = reply.ConflictIndex
		} else if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}
}

// maybeAdvanceCommitIndexLocked implements the "majority replication" rule:
// commit any entry from the current term that's stored on a majority of nodes.
func (r *Raft) maybeAdvanceCommitIndexLocked() {
	for n := len(r.log) - 1; n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count > (len(r.peers)+1)/2 {
			r.commitIndex = n
			r.applyCommittedLocked()
			break
		}
	}
}

func (r *Raft) applyCommittedLocked() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.log[r.lastApplied]
		if r.apply != nil {
			go r.apply(entry)
		}
	}
}

// --- Inbound RPC handlers, called by the transport layer ---

func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.alive {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	reply := &RequestVoteReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term
	logOK := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (r.votedFor == "" || r.votedFor == args.CandidateID) && logOK {
		r.votedFor = args.CandidateID
		reply.VoteGranted = true
		r.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}
	return reply
}

func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.alive {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	reply := &AppendEntriesReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.Success = false
		return reply
	}

	if args.Term > r.currentTerm || r.state != Follower {
		r.becomeFollower(args.Term)
	}
	r.leaderID = args.LeaderID
	r.resetElectionTimer()
	reply.Term = r.currentTerm

	// Consistency check on PrevLogIndex/PrevLogTerm.
	if args.PrevLogIndex >= len(r.log) {
		reply.Success = false
		reply.ConflictIndex = len(r.log)
		reply.ConflictTerm = -1
		return reply
	}
	if args.PrevLogIndex >= 0 && r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := r.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && r.log[idx-1].Term == conflictTerm {
			idx--
		}
		reply.Success = false
		reply.ConflictIndex = idx
		reply.ConflictTerm = conflictTerm
		return reply
	}

	// Append new entries, truncating on conflict.
	insertAt := args.PrevLogIndex + 1
	for i, e := range args.Entries {
		idx := insertAt + i
		if idx < len(r.log) {
			if r.log[idx].Term != e.Term {
				r.log = r.log[:idx]
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
		} else {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNew
		}
		r.applyCommittedLocked()
	}

	reply.Success = true
	return reply
}

// --- Client-facing API ---

// Propose appends a new command to the leader's log. Returns false if this
// node isn't the leader (caller should redirect the client).
func (r *Raft) Propose(command string) (index int, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   len(r.log),
		Command: command,
	}
	r.log = append(r.log, entry)
	return entry.Index, entry.Term, true
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	logCopy := make([]LogEntry, len(r.log))
	copy(logCopy, r.log)
	return Status{
		ID:          r.id,
		State:       r.state.String(),
		Term:        r.currentTerm,
		Leader:      r.leaderID,
		VotedFor:    r.votedFor,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		LogLength:   len(r.log) - 1,
		Log:         logCopy[1:],
		Peers:       r.peers,
		Alive:       r.alive,
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

func (r *Raft) LeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}
