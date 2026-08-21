FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /raft-node ./cmd/node

FROM alpine:3.20
WORKDIR /app
COPY --from=build /raft-node /app/raft-node
COPY web ./web
ENTRYPOINT ["/app/raft-node"]
