package kv

import (
	"strings"
	"sync"
)

// Store is a simple in-memory key-value map. It's the "state machine" in
// Raft terms: it only ever changes in response to committed log entries,
// which guarantees every node ends up with identical state.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply parses and executes a committed command. Commands are simple
// space-separated strings: "SET key value" or "DEL key".
func (s *Store) Apply(command string) {
	parts := strings.SplitN(command, " ", 3)
	if len(parts) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) == 3 {
			s.data[parts[1]] = parts[2]
		}
	case "DEL":
		if len(parts) >= 2 {
			delete(s.data, parts[1])
		}
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) All() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
