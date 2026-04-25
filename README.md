# Substrate Events — The Global Hash Event Server

GitHub IS the events server. This repository is the blob manifest for every Substrate instance on the mesh.

## What This Is

Every MythOS Substrate instance runs the same genesis schema — two tables (`substrate.blob`, `substrate.signal`), seven laws (BobsBlobLaws). Blobs are things that exist. Signals are things that happened. The `subscriber` column on every blob determines where it replicates.

This repository is the **sync wire**. When a blob changes on any Substrate instance:

```
Blob changes on Machine A
  → PG NOTIFY fires locally (Law 5: Radiation)
  → Babel pushes updated hash entry to this repo
  → GitHub webhook fires (or Babels poll)
  → Each Babel checks: is my principal in the subscriber list?
  → If yes: pull blob data from Machine A over WireGuard
  → If no: ignore
```

**Zero custom global infrastructure.** GitHub provides:
- **Hash registry** — the manifest is content-addressable, versioned, auditable
- **Event bus** — every `git push` is an event; webhooks notify subscribers
- **Bootstrap source** — a fresh machine clones this repo, reads the manifest, pulls blobs from peers
- **Audit trail** — `git log` IS the replication history
- **Global availability** — GitHub's CDN, free, already distributed

The OS that uses the git model for its data plane uses actual git as the sync bus.

## Structure

```
manifest/
  <peer_id>.json         — one file per Substrate peer
events/
  <YYYY-MM-DD>.jsonl     — daily event log (append-only)
peers/
  registry.json          — known peers and their WireGuard endpoints
```

### Manifest Entry (per blob)

```json
{
  "unid": "uuid",
  "content_hash": "sha256 hex",
  "ordinal": 42,
  "composition": "file",
  "name": "claude.exe",
  "size": 245230208,
  "subscriber": ["SYSTEM", "joey", "oa:matt"],
  "origin_peer": "mythserv1",
  "origin_endpoint": "10.69.0.2",
  "updated_at": "2026-04-25T03:00:00Z"
}
```

The manifest entry contains **metadata only** — never blob content. Blob data flows peer-to-peer over WireGuard. GitHub routes notifications; it never touches the actual bytes.

### Event Entry

```json
{
  "event_id": "uuid",
  "blob_unid": "uuid",
  "signal_type": "ingest",
  "content_hash": "sha256 hex",
  "ordinal": 42,
  "subscriber": ["SYSTEM"],
  "origin_peer": "mythserv1",
  "timestamp": "2026-04-25T03:00:00Z"
}
```

### Peer Registry

```json
{
  "peers": {
    "mythserv1": {
      "endpoint": "10.69.0.2",
      "wg_public_key": "...",
      "substrate_version": "genesis-v1",
      "last_seen": "2026-04-25T03:00:00Z",
      "blob_count": 87
    }
  }
}
```

## How Sync Works

### Push (blob changed locally)

```sql
-- Inside PostgreSQL, after a blob changes:
SELECT substrate.publish(blob_unid);
-- This calls out to: git add, git commit, git push
-- Or: GitHub API to update the manifest file
```

### Poll (check for remote changes)

```sql
-- Babel's event poller runs periodically:
SELECT substrate.poll_events();
-- Pulls latest from this repo
-- For each new event where my principal is in subscriber list:
--   Fetch blob from origin_peer over WireGuard
--   INSERT into local Substrate
```

### Subscribe (new peer joins)

```sql
-- Fresh machine runs genesis, then:
SELECT substrate.sync_from_manifest();
-- Clones this repo
-- Reads all manifest entries
-- For each where my principal is in subscriber list:
--   Fetch blob from origin_peer over WireGuard
--   INSERT into local Substrate
```

## The Subscriber IS the Routing Table

The `subscriber` array on each manifest entry determines who gets notified and who pulls:

| Subscriber value | Who gets it |
|-----------------|-------------|
| `SYSTEM` | Every Substrate peer. Universal. |
| `joey` | Joey's Substrate instance |
| `oa:matt` | Matt's Substrate on MythServ1 |
| `tenant:horizenit` | Every machine in the HorizenIT fog |

The same field that controls access in RLS (Law 7: Topology) controls replication across the mesh. One field. One truth.

## Compression + Transport

Blobs can be compressed before emission. The `substrate.compress()` force reduces a blob to ~30% of original size. The `substrate.emit()` force handles delivery via any protocol. The subscription blob describes the full delivery contract: who, what, where, how, when, whether to compress.

## What This Proves

The hardest problems in distributed systems — ordering, integrity, conflicts, partitioning, consensus — are already solved by primitives that exist for other reasons:

- **Ordering**: monotonic ordinals
- **Integrity**: content hashing (Law 1)
- **Conflicts**: blobs are enrolled/retired, not mutated; signals are append-only (Laws 2, 4)
- **Partitioning**: subscriber field bounds the replication set
- **Authentication**: WireGuard already did it
- **Real-time**: PG NOTIFY locally, GitHub webhooks globally
- **Consensus**: not needed — this is git, not Paxos

---

*The metaphor is the implementation. Full circle.*
