import argparse
import asyncio
import json
import random
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Dict, Any, Optional, List

import websockets
from pysyncobj import SyncObj, SyncObjConf, replicated


@dataclass
class Player:
    client_id: str
    name: str
    hp: int = 100


@dataclass
class GameState:
    players: Dict[str, Player] = field(default_factory=dict)


class TabletopServer(SyncObj):
    def __init__(self, self_addr: str, partner_addrs: List[str], journal_file: str, audit_log_path: Path) -> None:
        conf = SyncObjConf( journalFile=journal_file, dynamicMembershipChange=True)
        super().__init__(self_addr, partner_addrs, conf)

        self.self_addr = self_addr
        self.clients: Dict[str, "websockets.WebSocketServerProtocol"] = {}
        self.seq: int = 0
        self.state = GameState()
        self.dedup: Dict[str, int] = {}
        self.event_history: list[Dict[str, Any]] = []
        self.audit_log_path = audit_log_path
        print(f"[startup] TabletopServer __init__ done. self_addr={self.self_addr} audit_log={self.audit_log_path.resolve()}", flush=True)

    def append_event_to_log(self, event: Dict[str, Any]) -> None:
        try:
            print(
                f"[audit] writing seq={event.get('seq')} to {self.audit_log_path.resolve()}",
                flush=True,
            )
            with self.audit_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")
        except Exception as e:
            print(f"[audit] FAILED: {type(e).__name__}: {e}", flush=True)
            raise

    def apply_event(self, event: Dict[str, Any]) -> None:
        event_type = event.get("event_type")
        payload = event.get("payload", {})

        if event_type == "JOIN":
            client_id = payload["client_id"]
            name = payload["name"]

            if client_id not in self.state.players:
                self.state.players[client_id] = Player(client_id=client_id, name=name)
            else:
                self.state.players[client_id].name = name

        elif event_type == "SET_HP":
            target_id = payload["target_id"]
            new_hp = payload["new_hp"]

            if target_id not in self.state.players:
                self.state.players[target_id] = Player(
                    client_id=target_id,
                    name=target_id,
                    hp=new_hp,
                )
            else:
                self.state.players[target_id].hp = new_hp

        elif event_type in {"CHAT", "ROLL_DICE"}:
            pass

        else:
            print(f"Warning: unknown event_type during replay: {event_type}", flush=True)

    @replicated
    def _commit_event( self, dedup_key: str, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Only method allowed to touch self.seq, self.dedup, self.state, or self.event_history.

        Returns the fully-stamped event (with seq populated) on success,
        or None if this command was a duplicate that should be ignored.
        """
        print(f"[commit] enter dedup={dedup_key} type={event.get('event_type')}", flush=True)
        if dedup_key in self.dedup:
            return None
        
        # SET_HP's new hp depends on state, computed inside @replicated so concurrent SET_HP commands compose correctly.
        if event["event_type"] == "SET_HP":
            target_id = event["payload"]["target_id"]
            delta = event["payload"]["delta"]
            current_hp = 100
            if target_id in self.state.players:
                current_hp = self.state.players[target_id].hp
            event["payload"]["new_hp"] = max(0, current_hp + delta)

        self.seq += 1
        event["seq"] = self.seq

        self.apply_event(event)
        self.append_event_to_log(event)
        self.dedup[dedup_key] = self.seq
        self.event_history.append(event)

        return event

    async def broadcast(self, msg: Dict[str, Any]) -> None:
        data = json.dumps(msg)
        dead = []

        for cid, ws in self.clients.items():
            try:
                await ws.send(data)
            except Exception:
                dead.append(cid)

        for cid in dead:
            self.clients.pop(cid, None)

    def validate_command(self, obj: Dict[str, Any]) -> Optional[str]:
        if not isinstance(obj, dict):
            return "Message must be a JSON object."
        if obj.get("type") != "command":
            return "Expected type='command'."
        if not isinstance(obj.get("client_id"), str) or not obj["client_id"]:
            return "Missing/invalid client_id."
        if not isinstance(obj.get("event_id"), str) or not obj["event_id"]:
            return "Missing/invalid event_id."
        if not isinstance(obj.get("command"), str) or not obj["command"]:
            return "Missing/invalid command."
        if "payload" not in obj or not isinstance(obj["payload"], dict):
            return "Missing/invalid payload (must be object)."
        return None

    def make_error(self, message: str) -> Dict[str, Any]:
        return {"type": "error", "message": message}

    async def send_error_to(self, client_id: str, message: str) -> None:
        ws = self.clients.get(client_id)
        if ws is None:
            return
        try:
            await ws.send(json.dumps(self.make_error(message)))
        except Exception:
            pass

    async def handle_command(self, cmd: Dict[str, Any]) -> None:
        client_id: str = cmd["client_id"]
        event_id: str = cmd["event_id"]
        command: str = cmd["command"]
        payload: Dict[str, Any] = cmd["payload"]

        # --- Leader check (only the leader may accept commands) ---
        leader = self._getLeader()
        if leader is None:
            await self.send_error_to(
                client_id, "Cluster initializing; please retry shortly."
            )
            return
        if str(leader) != self.self_addr:
            await self.send_error_to(
                client_id, f"Not leader; reconnect to {leader}"
            )
            return

        # --- Per-command validation and any leader-side computation ---
        if command == "JOIN":
            name = payload.get("name")
            if not isinstance(name, str) or not name.strip():
                await self.send_error_to(
                    client_id, "JOIN requires payload.name as non-empty string."
                )
                return
            event = {
                "type": "event",
                "event_id": event_id,
                "event_type": "JOIN",
                "payload": {"client_id": client_id, "name": name.strip()},
            }

        elif command == "CHAT":
            text = payload.get("text")
            if not isinstance(text, str) or not text.strip():
                await self.send_error_to(
                    client_id, "CHAT requires payload.text as non-empty string."
                )
                return
            event = {
                "type": "event",
                "event_id": event_id,
                "event_type": "CHAT",
                "payload": {"client_id": client_id, "text": text},
            }

        elif command == "ROLL_DICE":
            sides = payload.get("sides")
            if not isinstance(sides, int) or sides < 2 or sides > 10_000:
                await self.send_error_to(
                    client_id, "ROLL_DICE requires payload.sides as int >= 2."
                )
                return
            # Roll the dice on the leader; replicas receive the result via Raft.
            result = random.randint(1, sides)
            event = {
                "type": "event",
                "event_id": event_id,
                "event_type": "ROLL_DICE",
                "payload": {
                    "client_id": client_id,
                    "sides": sides,
                    "result": result,
                },
            }

        elif command == "SET_HP":
            target_id = payload.get("target_id")
            delta = payload.get("delta")
            if not isinstance(target_id, str) or not target_id:
                await self.send_error_to(
                    client_id,
                    "SET_HP requires payload.target_id as non-empty string.",
                )
                return
            if not isinstance(delta, int):
                await self.send_error_to(
                    client_id, "SET_HP requires payload.delta as integer."
                )
                return
            # new_hp is computed inside _commit_event against post-consensus state.
            event = {
                "type": "event",
                "event_id": event_id,
                "event_type": "SET_HP",
                "payload": {"target_id": target_id, "delta": delta},
            }

        else:
            await self.send_error_to(client_id, f"Unknown command: {command}")
            return

        dedup_key = f"{client_id}:{event_id}"

        # --- Replicate via Raft (blocking; run on executor to keep loop responsive) ---
        loop = asyncio.get_event_loop()
        try:
            committed = await loop.run_in_executor(
                None,
                partial(
                    self._commit_event,
                    dedup_key,
                    event,
                    sync=True,
                    timeout=5.0,
                ),
            )
        except Exception as e:
            await self.send_error_to(client_id, f"Replication failed: {e}")
            return

        if committed is None:
            print(f"Duplicate command ignored: {dedup_key}", flush=True)
            return

        print(
            f"Emitting event: seq={committed['seq']}, "
            f"type={committed['event_type']}, event_id={event_id}",
            flush=True,
        )
        await self.broadcast(committed)

    async def handler(self, ws: websockets.WebSocketServerProtocol) -> None:
        registered_client_id: Optional[str] = None

        try:
            async for raw in ws:
                try:
                    obj = json.loads(raw)
                except Exception:
                    await ws.send(json.dumps(self.make_error("Invalid JSON.")))
                    continue

                err = self.validate_command(obj)
                if err:
                    await ws.send(json.dumps(self.make_error(err)))
                    continue

                client_id = obj["client_id"]
                registered_client_id = client_id
                self.clients[client_id] = ws

                await self.handle_command(obj)

        except websockets.ConnectionClosed:
            pass
        finally:
            if registered_client_id and self.clients.get(registered_client_id) is ws:
                self.clients.pop(registered_client_id, None)

    async def run(self, host: str, port: int) -> None:
        print(f"Server starting on ws://{host}:{port}", flush=True)
        async with websockets.serve(self.handler, host, port, reuse_port=True):
            await asyncio.Future()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replicated D&D tabletop server")
    parser.add_argument(
        "--ws-host",
        default="0.0.0.0",
        help="Host/interface for the WebSocket server (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--ws-port",
        type=int,
        default=8765,
        help="Port for the WebSocket server (default: 8765)",
    )
    parser.add_argument(
        "--raft-addr",
        required=True,
        help="This node's Raft address, e.g. localhost:9001",
    )
    parser.add_argument(
        "--raft-peers",
        required=True,
        help="Comma-separated Raft addresses of the OTHER replicas, "
             "e.g. localhost:9002,localhost:9003",
    )
    parser.add_argument(
        "--journal",
        required=True,
        help="Path to pysyncobj binary journal file for this node",
    )
    parser.add_argument(
        "--audit-log",
        required=True,
        help="Path to human-readable events.jsonl for this node",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    partner_addrs = [p.strip() for p in args.raft_peers.split(",") if p.strip()]

    server = TabletopServer(
        self_addr=args.raft_addr,
        partner_addrs=partner_addrs,
        journal_file=args.journal,
        audit_log_path=Path(args.audit_log),
    )

    await server.run(args.ws_host, args.ws_port)


if __name__ == "__main__":
    asyncio.run(main())