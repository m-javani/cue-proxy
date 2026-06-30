# CueProxy

**CueProxy** is the stateless HTTP and WebSocket gateway for [Cue](https://github.com/m-javani/cue) - a distributed, in-memory job queue with Raft-based consistency.

CueProxy handles all external communication, allowing you to:
- **Submit jobs** via HTTP REST API
- **Consume jobs** via WebSocket connections
- **Scale horizontally** without any cluster coordination
- **Authenticate** producers and consumers with simple token-based auth

> **⚠️ CueProxy is a companion to the Cue cluster. Neither works alone.**
> 
> You must have a running Cue cluster (3/5/7 nodes) before CueProxy can serve traffic.

---

[![CI](https://github.com/m-javani/cue-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/m-javani/cue-proxy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/m-javani/cue-proxy/graph/badge.svg)](https://app.codecov.io/gh/m-javani/cue-proxy)
[![Go Report Card](https://goreportcard.com/badge/github.com/m-javani/cue-proxy)](https://goreportcard.com/report/github.com/m-javani/cue-proxy)
[![Go Version](https://img.shields.io/badge/Go-1.26.3-blue)](https://golang.org/)
[![Docker Pulls](https://img.shields.io/docker/pulls/m-javani/cue-proxy)](https://hub.docker.com/r/m-javani/cue-proxy)

---

## Table of Contents

- [Full Docs](#docs)
- [How It Works](#how-it-works)
- [Key Features](#key-features)
- [Limitations](#limitations)
- [Contribution](#contributing)
- [License](#license)

---

## Docs 
> read the full docs [here](https://m-javani.github.io/cue-docs/).

**Related Project:**
- [Cue Cluster](https://github.com/m-javani/cue) - The distributed job queue

---

## How It Works

```
                    ┌─────────────────────────────────────────────────┐
                    │                 Cue Cluster                    │
                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
                    │  │ Leader  │◀─▶│Follower │◀─▶│Follower │       │
                    │  └─────────┘  └─────────┘  └─────────┘       │
                    └───────────▲─────────────────▲─────────────────┘
                                │ QUIC            │ QUIC
                    ┌───────────┴─────────┐┌─────┴──────────────┐
                    │                     ││                     │
              ┌─────▼─────┐         ┌─────▼─────┐
              │ CueProxy  │         │ CueProxy  │
              │ Instance  │         │ Instance  │
              └─────┬─────┘         └─────┬─────┘
                    │                     │
        ┌───────────┼───────────┐         │
        │           │           │         │
   ┌────▼───┐ ┌────▼───┐ ┌────▼───┐     │
   │Consumer│ │Consumer│ │Consumer│     │
   │  A     │ │  B     │ │  C     │     │
   └────────┘ └────────┘ └────────┘     │
                                         │
                                    ┌────▼────┐
                                    │Consumer │
                                    │  D      │
                                    └─────────┘
```

**Each CueProxy instance is completely independent** - no coordination between proxies. They all connect directly to the Cue cluster leader.

**How it works:**

1. **Producer** submits a job via HTTP to any CueProxy instance
2. That CueProxy forwards it to the **Cue cluster leader** via QUIC
3. The leader persists the job and dispatches it to **any proxy** that has consumers for the topic
4. The proxy delivers the job to one of its connected **consumers** for that topic
5. Consumer processes the job and sends `ack` via WebSocket

**Multiple proxies and consumers:**

- The leader **load balances** dispatching across all proxies that have consumers for a topic
- Each proxy can serve **many consumers**, even for the same topic
- Multiple consumers on the same topic will **share the workload**

**Resilience & Idempotency:**

- If a CueProxy dies, its consumers can **reconnect to any other running proxy** with the same UUID and topic
- **Consumers must be idempotent** - if a job is retried due to a missed `ack`, it will be redelivered
- When a consumer receives a duplicate job, it should **ignore it and send `ack`** to prevent further retries or dead-letter queue (DLQ)

> **Note:** The cluster dispatches jobs to **any proxy** that has consumers for the topic. The leader load balances across all eligible proxies.
>
> **Producer responses:** Sync HTTP responses always go back to the proxy that submitted the job, which is waiting for the reply.
>
> **If a proxy dies:**
> - In-flight responses to producers are lost. Producers should **retry with another proxy**
> - If the job was already persisted before the proxy died, the producer's retry will attempt to **duplicate the job**
> - **The leader detects the duplicate** - if the job ID already exists and is still in memory, it returns an **"already exists"** error to the new proxy
> - If the job was already dispatched and acknowledged, the cluster may not have it in memory anymore - the consumer handles this via **idempotency** (detects duplicate `job_id`, ignores it, and sends `ack`)
>
> **Flexibility:** Producers and consumers can use **different proxies** - they don't need to be connected to the same instance. Each proxy operates independently.

---

## Key Features

### 🚀 Independent Horizontal Scaling
CueProxy instances are completely stateless. Add more proxies behind a load balancer to handle more concurrent connections - no cluster coordination required.

### 🔐 Simple Authentication
Token-based authentication with roles (`producer`, `consumer`, `admin`, `monitoring`). Tokens are loaded from a YAML file with automatic reloading.

### 🔒 Secure Communication
- TLS/HTTPS for HTTP endpoints
- TLS-secured QUIC connections to the Cue cluster
- mTLS support with configurable certificate verification
- Multiple TLS verifier strategies: CN, DNS, SPIFFE

### 🧭 Dynamic Service Discovery
CueProxy automatically discovers the Cue cluster topology:
- **Static**: Provide a list of node addresses
- **DNS**: Resolve a domain to node addresses
- **Service**: Use service discovery (e.g., Consul)

No manual configuration when nodes join or leave - CueProxy detects changes via heartbeat messages.

### 📊 Built-in Monitoring
- Prometheus `/metrics` endpoint
- Health check endpoint
- Metrics for connections, requests, and errors

## Limitations

CueProxy inherits the limitations of the Cue cluster:

- **In-memory only**: Jobs are not persisted beyond what Raft WAL provides
- **Bounded queues**: Configurable `max_jobs_per_topic` (default: 10,000)
- **No replay**: Once acknowledged, jobs are removed from memory
- **Leader dependency**: All writes go through the cluster leader
- **Throughput**: Limited by leader's memory and Raft consensus

**Cue is not a Kafka replacement.** For large-scale persistent streaming, use Kafka. For simple fire-and-forget, use Redis Pub/Sub.

Cue is for teams who need reliable job dispatch with automatic retries and dead letter handling without operating complex infrastructure.

## Contributing

Contributions are welcome! Please read the [contributing guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.

---
