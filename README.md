# raft-kv

A distributed key-value store built on a from-scratch Raft consensus implementation in Go.

## What's implemented

- **Leader election** — randomized election timeouts, RequestVote RPC, term-based safety
- **Log replication** — AppendEntries RPC, majority-commit rule, conflict resolution/backtracking on log mismatch
- **Failure handling** — a killed leader triggers automatic re-election; a revived node catches up on missed log entries
- **KV state machine** — SET/GET/DEL applied only once entries are committed (majority-replicated)
- **Live dashboard** — see term, leader, log, and commit index across the cluster in real time, with Kill/Revive buttons per node to demo failover

## Architecture

```
cmd/node/main.go        - wires everything together, HTTP server (RPC + client API + chaos endpoints)
internal/raft/          - the actual Raft implementation (types, transport interface, core algorithm)
internal/kv/            - in-memory KV store, applied from the committed log
internal/transport/     - HTTP+JSON transport for peer-to-peer RPCs
web/                     - live dashboard (vanilla JS, polls /status every 500ms)
```

## Running locally (no Docker needed)

```bash
go build -o raft-node ./cmd/node

./raft-node -id=node1 -addr=:8001 -peers="node1=localhost:8001,node2=localhost:8002,node3=localhost:8003" &
./raft-node -id=node2 -addr=:8002 -peers="node1=localhost:8001,node2=localhost:8002,node3=localhost:8003" &
./raft-node -id=node3 -addr=:8003 -peers="node1=localhost:8001,node2=localhost:8002,node3=localhost:8003" &
```

Open `web/index.html` in a browser, or serve it via any of the running nodes at `http://localhost:8001/`.

## Running with Docker Compose

```bash
docker compose up --build
```

Dashboard at `http://localhost:8001/`.

## API

- `POST /set?key=K&value=V` — write (redirects with `leader` field if not sent to the current leader)
- `GET /get?key=K` — read
- `GET /status` — full node status (term, state, log, commit index)
- `POST /admin/kill` / `POST /admin/revive` — chaos endpoints for demoing failover

## Why this project

Built as a portfolio piece for SDE roles — demonstrates distributed systems fundamentals
(consensus, replication, failure recovery) that most CRUD-project portfolios don't touch.
