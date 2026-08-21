package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"raftkv/internal/kv"
	"raftkv/internal/raft"
	"raftkv/internal/transport"
)

func main() {
	id := flag.String("id", "", "this node's id, e.g. node1")
	httpAddr := flag.String("addr", ":8001", "address to listen on, e.g. :8001")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port pairs for ALL nodes including self, e.g. node1=node1:8001,node2=node2:8002,node3=node3:8003")
	flag.Parse()

	if *id == "" || *peersFlag == "" {
		log.Fatal("must supply -id and -peers")
	}

	addrOf := map[string]string{}
	var peerIDs []string
	for _, p := range strings.Split(*peersFlag, ",") {
		kvpair := strings.SplitN(p, "=", 2)
		if len(kvpair) != 2 {
			log.Fatalf("bad peer spec %q", p)
		}
		addrOf[kvpair[0]] = kvpair[1]
		if kvpair[0] != *id {
			peerIDs = append(peerIDs, kvpair[0])
		}
	}

	store := kv.NewStore()
	tport := transport.NewHTTPTransport(addrOf)

	applyFn := func(entry raft.LogEntry) {
		store.Apply(entry.Command)
	}

	r := raft.NewRaft(*id, peerIDs, tport, applyFn)

	var alive int32 = 1 // chaos-testing toggle: 0 = simulated dead node

	mux := http.NewServeMux()

	// --- Raft peer RPCs ---
	mux.HandleFunc("/raft/requestvote", func(w http.ResponseWriter, req *http.Request) {
		if atomic.LoadInt32(&alive) == 0 {
			http.Error(w, "node down", http.StatusServiceUnavailable)
			return
		}
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := r.HandleRequestVote(&args)
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/appendentries", func(w http.ResponseWriter, req *http.Request) {
		if atomic.LoadInt32(&alive) == 0 {
			http.Error(w, "node down", http.StatusServiceUnavailable)
			return
		}
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := r.HandleAppendEntries(&args)
		json.NewEncoder(w).Encode(reply)
	})

	// --- Status / dashboard ---
	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		status := r.Status()
		status.Alive = atomic.LoadInt32(&alive) == 1
		json.NewEncoder(w).Encode(status)
	})

	// --- Chaos endpoints for the demo ---
	mux.HandleFunc("/admin/kill", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		atomic.StoreInt32(&alive, 0)
		r.SetAlive(false)
		fmt.Fprintf(w, "node %s marked DOWN\n", *id)
	})
	mux.HandleFunc("/admin/revive", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		atomic.StoreInt32(&alive, 1)
		r.SetAlive(true)
		fmt.Fprintf(w, "node %s marked UP\n", *id)
	})

	// --- Client API: SET / GET, redirects to leader if needed ---
	mux.HandleFunc("/set", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if atomic.LoadInt32(&alive) == 0 {
			http.Error(w, "node down", http.StatusServiceUnavailable)
			return
		}
		key := req.URL.Query().Get("key")
		val := req.URL.Query().Get("value")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		idx, term, isLeader := r.Propose(fmt.Sprintf("SET %s %s", key, val))
		if !isLeader {
			leader := r.LeaderID()
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": leader,
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok": true, "index": idx, "term": term,
		})
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if atomic.LoadInt32(&alive) == 0 {
			http.Error(w, "node down", http.StatusServiceUnavailable)
			return
		}
		key := req.URL.Query().Get("key")
		val, ok := store.Get(key)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key": key, "value": val, "found": ok,
		})
	})

	mux.Handle("/", http.FileServer(http.Dir("./web")))

	r.Start()
	log.Printf("node %s listening on %s, peers=%v", *id, *httpAddr, peerIDs)
	log.Fatal(http.ListenAndServe(*httpAddr, mux))
}
