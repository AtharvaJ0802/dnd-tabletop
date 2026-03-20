import asyncio
import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional

import websockets


LOG_FILE = Path("events.jsonl")


@dataclass
class Player:
    client_id: str
    name: str
    hp: int = 100


@dataclass
class GameState:
    players: Dict[str, Player] = field(default_factory=dict)


class TabletopServer:
    def __init__(self) -> None:
        self.clients: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.seq: int = 0
        self.state = GameState()
        self.dedup: Dict[str, int] = {}
        self._shutdown = asyncio.Event()
        self.event_history: list[Dict[str, Any]] = []

        self.load_and_replay_log()

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def load_and_replay_log(self) -> None:
        if not LOG_FILE.exists():
            print("No existing event log found. Starting fresh.", flush=True)
            return

        print(f"Replaying event log from {LOG_FILE}...", flush=True)
        replayed = 0

        with LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                event = json.loads(line)
                self.apply_event(event)
                self.seq = max(self.seq, event["seq"])
                replayed += 1

        self.event_history.append(event)

        print(f"Replayed {replayed} events. Current seq={self.seq}", flush=True)

    def append_event_to_log(self, event: Dict[str, Any]) -> None:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

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

        dedup_key = f"{client_id}:{event_id}"
        if dedup_key in self.dedup:
            print(f"Duplicate command ignored: {dedup_key}", flush=True)
            return

        seq = self.next_seq()

        print(
            f"{seq} Processing command from {client_id}: {command} with payload {payload}",
            flush=True,
        )

        if command == "JOIN":
            name = payload.get("name")
            if not isinstance(name, str) or not name.strip():
                await self.send_error_to(
                    client_id, "JOIN requires payload.name as non-empty string."
                )
                self.seq -= 1
                return

            event = {
                "type": "event",
                "seq": seq,
                "event_id": event_id,
                "event_type": "JOIN",
                "payload": {
                    "client_id": client_id,
                    "name": name.strip(),
                },
            }

        elif command == "CHAT":
            text = payload.get("text")
            if not isinstance(text, str) or not text.strip():
                await self.send_error_to(
                    client_id, "CHAT requires payload.text as non-empty string."
                )
                self.seq -= 1
                return

            event = {
                "type": "event",
                "seq": seq,
                "event_id": event_id,
                "event_type": "CHAT",
                "payload": {
                    "client_id": client_id,
                    "text": text,
                },
            }

        elif command == "ROLL_DICE":
            sides = payload.get("sides")
            if not isinstance(sides, int) or sides < 2 or sides > 10_000:
                await self.send_error_to(
                    client_id, "ROLL_DICE requires payload.sides as int >= 2."
                )
                self.seq -= 1
                return

            result = random.randint(1, sides)

            event = {
                "type": "event",
                "seq": seq,
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
                self.seq -= 1
                return

            if not isinstance(delta, int):
                await self.send_error_to(
                    client_id,
                    "SET_HP requires payload.delta as integer.",
                )
                self.seq -= 1
                return

            current_hp = 20
            if target_id in self.state.players:
                current_hp = self.state.players[target_id].hp

            new_hp = max(0, current_hp + delta)

            event = {
                "type": "event",
                "seq": seq,
                "event_id": event_id,
                "event_type": "SET_HP",
                "payload": {
                    "target_id": target_id,
                    "delta": delta,
                    "new_hp": new_hp,
                },
            }

        else:
            await self.send_error_to(client_id, f"Unknown command: {command}")
            self.seq -= 1
            return

        self.apply_event(event)
        self.append_event_to_log(event)
        self.dedup[dedup_key] = seq
        self.event_history.append(event)
        
        print(
            f"Emitting event: seq={seq}, type={event['event_type']}, event_id={event_id}",
            flush=True,
        )

        await self.broadcast(event)

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


async def main() -> None:
    server = TabletopServer()
    await server.run("0.0.0.0", 8765)


if __name__ == "__main__":
    asyncio.run(main())