package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"raftkv/internal/raft"
)

// HTTPTransport sends Raft RPCs over plain HTTP+JSON. Peer addresses are
// "host:port" strings; peer IDs and addresses are assumed identical here
// for simplicity (see cmd/node for the id->addr map).
type HTTPTransport struct {
	client   *http.Client
	addrOf   map[string]string // peer id -> host:port
}

func NewHTTPTransport(addrOf map[string]string) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 200 * time.Millisecond},
		addrOf: addrOf,
	}
}

func (t *HTTPTransport) post(peer, path string, body interface{}, out interface{}) error {
	addr, ok := t.addrOf[peer]
	if !ok {
		return fmt.Errorf("unknown peer %s", peer)
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(fmt.Sprintf("http://%s%s", addr, path), "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t *HTTPTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := t.post(peer, "/raft/requestvote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := t.post(peer, "/raft/appendentries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}
