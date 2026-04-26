import argparse
import asyncio
import json
import sys
import uuid
from typing import Dict, Any, Optional, List

import websockets


def make_event_id() -> str:
    return str(uuid.uuid4())


def pretty_event(evt: Dict[str, Any]) -> str:
    et = evt.get("event_type", "UNKNOWN")
    seq = evt.get("seq", "?")
    payload = evt.get("payload", {})
    return f"[seq={seq}] {et} {payload}"


class ClientState:
    """Holds everything one client session needs: known replicas, current
    connection, pending (in-flight) commands, last seq seen, and the most
    recent name used for JOIN."""

    def __init__(self, uris: List[str], client_id: str) -> None:
        self.uris = uris
        self.uri_idx = 0
        self.client_id = client_id
        self.pending: Dict[str, Dict[str, Any]] = {}  # event_id -> command JSON
        self.last_seq_seen = 0
        self.name: Optional[str] = None
        self.ws: Optional[Any] = None

    def current_uri(self) -> str:
        return self.uris[self.uri_idx]

    def rotate(self) -> None:
        self.uri_idx = (self.uri_idx + 1) % len(self.uris)


async def try_connect(state: ClientState) -> bool:
    """Walk through URIs in rotation until one accepts a TCP/WS connection.
    Doesn't yet know whether it's the leader; that surfaces on first command."""
    attempts = 0
    max_attempts = len(state.uris) * 3

    while attempts < max_attempts:
        uri = state.current_uri()
        try:
            print(f"[net] connecting to {uri}...")
            state.ws = await websockets.connect(uri, open_timeout=3.0)
            print(f"[net] connected to {uri}")
            return True
        except Exception as e:
            print(f"[net] connect to {uri} failed: {e}")
            state.rotate()
            attempts += 1
            await asyncio.sleep(0.5)

    return False


async def send_msg(state: ClientState, msg: Dict[str, Any]) -> bool:
    """Send a single JSON message on the current ws. Returns False if there
    is no usable connection."""
    if state.ws is None:
        return False
    try:
        await state.ws.send(json.dumps(msg))
        return True
    except Exception:
        return False


async def issue_command(
    state: ClientState,
    command: str,
    payload: Dict[str, Any],
    event_id: Optional[str] = None,
) -> str:
    """User-facing command issue. Adds to pending tracker (so it survives
    reconnects), then sends if connected."""
    if event_id is None:
        event_id = make_event_id()
    msg = {
        "type": "command",
        "client_id": state.client_id,
        "event_id": event_id,
        "command": command,
        "payload": payload,
    }
    state.pending[event_id] = msg
    sent = await send_msg(state, msg)
    if not sent:
        print(f"[queued] {command} pending until reconnect")
    return event_id


async def resend_pending(state: ClientState) -> None:
    """Re-send everything still in pending after a (re)connect. The server's
    {client_id}:{event_id} dedup ensures already-committed commands don't
    apply twice."""
    # If we'd JOINed before but have no JOIN in pending, send a fresh one so
    # the new leader registers our ws for broadcasts.
    if state.name is not None:
        join_in_pending = any(
            cmd["command"] == "JOIN" for cmd in state.pending.values()
        )
        if not join_in_pending:
            await issue_command(state, "JOIN", {"name": state.name})

    for event_id, msg in list(state.pending.items()):
        sent = await send_msg(state, msg)
        if sent:
            print(f"[resend] {msg['command']} event_id={event_id[:8]}")


async def receiver(state: ClientState) -> None:
    """Read events from current ws until it closes or returns a leader-redirect
    error. Pops acknowledged commands out of pending."""
    try:
        async for raw in state.ws:
            try:
                obj = json.loads(raw)
            except Exception:
                print("<< invalid JSON from server >>")
                continue

            if obj.get("type") == "error":
                msg = obj.get("message", "")
                print(f"ERROR: {msg}")
                if "Not leader" in msg or "initializing" in msg:
                    print("[net] leader hint received; will reconnect...")
                    try:
                        await state.ws.close()
                    except Exception:
                        pass
                    return
                continue

            if obj.get("type") == "event":
                seq = obj.get("seq")
                if isinstance(seq, int):
                    state.last_seq_seen = max(state.last_seq_seen, seq)
                event_id = obj.get("event_id")
                if event_id and event_id in state.pending:
                    del state.pending[event_id]
                print(pretty_event(obj))
                continue

            print(f"<< unknown message: {obj} >>")
    except Exception as e:
        print(f"[net] receiver error: {e}")


async def connection_loop(state: ClientState) -> None:
    """Maintain a working connection forever. On any failure, rotate URI,
    reconnect, resend pending."""
    while True:
        if not await try_connect(state):
            print("[net] could not reach any server. Retrying in 3s...")
            await asyncio.sleep(3)
            continue

        await resend_pending(state)
        await receiver(state)

        print("[net] disconnected, rotating to next replica...")
        state.ws = None
        state.rotate()
        await asyncio.sleep(0.5)


async def user_input_loop(state: ClientState) -> None:
    print("Commands:")
    print("  /join <name>")
    print("  /chat <text>")
    print("  /roll <sides>")
    print("  /hp <target_id> <delta>")
    print("  /quit")
    print()

    loop = asyncio.get_event_loop()

    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        line = line.strip()
        if not line:
            continue

        if line.startswith("/quit"):
            return

        if line.startswith("/join "):
            name = line[len("/join "):].strip()
            state.name = name
            await issue_command(state, "JOIN", {"name": name})
            continue

        if line.startswith("/chat "):
            text = line[len("/chat "):].strip()
            await issue_command(state, "CHAT", {"text": text})
            continue

        if line.startswith("/roll "):
            arg = line[len("/roll "):].strip()
            try:
                sides = int(arg)
            except ValueError:
                print("Usage: /roll <int sides>")
                continue
            await issue_command(state, "ROLL_DICE", {"sides": sides})
            continue

        if line.startswith("/hp "):
            parts = line.split()
            if len(parts) != 3:
                print("Usage: /hp <target_id> <delta>")
                continue
            target_id = parts[1]
            try:
                delta = int(parts[2])
            except ValueError:
                print("delta must be an integer")
                continue
            await issue_command(
                state, "SET_HP", {"target_id": target_id, "delta": delta}
            )
            continue

        print("Unknown command. Try /join, /chat, /roll, /hp, /quit")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tabletop client with multi-server failover"
    )
    parser.add_argument(
        "--servers",
        required=True,
        help="Comma-separated WebSocket URIs of all replicas, e.g. "
             "ws://localhost:8765,ws://localhost:8766,ws://localhost:8767",
    )
    parser.add_argument(
        "--client-id",
        default=None,
        help="Stable client ID (auto-generated if omitted)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    uris = [u.strip() for u in args.servers.split(",") if u.strip()]
    client_id = args.client_id or f"player-{uuid.uuid4().hex[:6]}"

    state = ClientState(uris, client_id)
    print(f"Client {client_id} starting; servers: {uris}")

    conn_task = asyncio.create_task(connection_loop(state))
    input_task = asyncio.create_task(user_input_loop(state))

    done, pending = await asyncio.wait(
        {conn_task, input_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()

    if state.ws:
        try:
            await state.ws.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())