# Protocol Specification
## Fault-Tolerant Real-Time Tabletop Session Service

This document defines the message protocol used between clients and servers
in the real-time tabletop session system.

All messages are encoded as JSON and transmitted over a persistent
WebSocket connection.

---

## Design Principles

- The server is **authoritative**: clients never modify game state directly.
- All state changes are represented as **immutable events**.
- Events are **totally ordered** using monotonically increasing sequence numbers.
- Clients may retry commands; servers must handle duplicates safely.
- The protocol is designed to support future replication and failover.

---

## Common Fields

### Identifiers
- `client_id` : string  
  Unique identifier assigned to a client (UUID or user-chosen string).

- `event_id` : string  
  Globally unique identifier for a command (UUID). Used for idempotency.

- `seq` : integer  
  Monotonically increasing sequence number assigned by the server.

---

## Message Types Overview

### Client -> Server
- `JOIN`
- `CHAT`
- `ROLL_DICE`
- `SET_HP`

### Server -> Client
- `EVENT`
- `ERROR`

---

## Client -> Server Messages (Commands)
### JOIN
```json
{
  "type": "command",
  "client_id": "player-123",
  "event_id": "uuid",
  "command": "JOIN",
  "payload": {
    "name": "Alice"
  }
}
```
### CHAT
```json
{
  "type": "command",
  "client_id": "player-123",
  "event_id": "uuid",
  "command": "CHAT",
  "payload": {
    "text": "Hello everyone!"
  }
}
```
### ROLL_DICE
```json
{
  "type": "command",
  "client_id": "player-123",
  "event_id": "uuid",
  "command": "ROLL_DICE",
  "payload": {
    "sides": 20
  }
}
```
### SET_HP
```json
{
  "type": "command",
  "client_id": "player-123",
  "event_id": "uuid",
  "command": "SET_HP",
  "payload": {
    "target_id": "player-123",
    "delta": -5
  }
}
```
---

## Server -> Client Messages (Events)
### CHAT
```json
{
  "type": "event",
  "seq": 5,
  "event_id": "uuid",
  "event_type": "CHAT",
  "payload": {
    "client_id": "player-123",
    "text": "Hello everyone!"
  }
}
```
### ROLL_DICE
```json
{
  "type": "event",
  "seq": 6,
  "event_id": "uuid",
  "event_type": "ROLL_DICE",
  "payload": {
    "client_id": "player-123",
    "sides": 20,
    "result": 17
  }
}
```

### SET_HP
```json
{
  "type": "event",
  "seq": 8,
  "event_id": "uuid",
  "event_type": "SET_HP",
  "payload": {
    "target_id": "player-123",
    "delta": -5,
    "new_hp": 17
  }
}

```

### JOIN
```json
{
  "type": "event",
  "seq": 1,
  "event_id": "uuid",
  "event_type": "JOIN",
  "payload": {
    "client_id": "player-123",
    "name": "Alice"
  }
}

```

### ERROR
```json
{
  "type": "error",
  "message": "Invalid command payload"
}
```
