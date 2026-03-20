import asyncio
import json
import sys
import uuid
from typing import Dict, Any, Optional

import websockets


def make_event_id() -> str:
    return str(uuid.uuid4())


def pretty_event(evt: Dict[str, Any]) -> str:
    et = evt.get("event_type", "UNKNOWN")
    seq = evt.get("seq", "?")
    payload = evt.get("payload", {})
    return f"[seq={seq}] {et} {payload}"


class ClientState:
    def __init__(self) -> None:
        self.last_seq_seen: int = 0
        self.name: Optional[str] = None


async def send_cmd(ws, client_id: str, command: str, payload: Dict[str, Any]) -> None:
    msg = {
        "type": "command",
        "client_id": client_id,
        "event_id": make_event_id(),
        "command": command,
        "payload": payload,
    }
    await ws.send(json.dumps(msg))


async def receiver(ws, state: ClientState) -> None:
    async for raw in ws:
        try:
            obj = json.loads(raw)
        except Exception:
            print("<< invalid JSON from server >>")
            continue

        if obj.get("type") == "error":
            print(f"ERROR: {obj.get('message')}")
            continue

        if obj.get("type") == "event":
            seq = obj.get("seq")
            if isinstance(seq, int):
                state.last_seq_seen = max(state.last_seq_seen, seq)
            print(pretty_event(obj))
            continue

        print(f"<< unknown message: {obj} >>")


async def user_input_loop(ws, client_id: str, state: ClientState) -> None:
    print("Commands:")
    print("  /join <name>")
    print("  /chat <text>")
    print("  /roll <sides>        (e.g., /roll 20)")
    print("  /hp <target_id> <delta>  (e.g., /hp player-123 -5)")
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
            await send_cmd(ws, client_id, "JOIN", {"name": name})
            continue

        if line.startswith("/chat "):
            text = line[len("/chat "):].strip()
            await send_cmd(ws, client_id, "CHAT", {"text": text})
            continue

        if line.startswith("/roll "):
            arg = line[len("/roll "):].strip()
            try:
                sides = int(arg)
            except ValueError:
                print("Usage: /roll <int sides>")
                continue
            await send_cmd(ws, client_id, "ROLL_DICE", {"sides": sides})
            continue

        if line.startswith("/hp "):
            parts = line.split()
            if len(parts) != 3 and len(parts) != 4:
                print("Usage: /hp <target_id> <delta>")
                print("   or: /hp <target_id> <delta>  (delta can be negative)")
                continue
            # Actually expects: /hp target_id delta
            if len(parts) != 3:
                # if someone typed extra spaces or words
                print("Usage: /hp <target_id> <delta>")
                continue
            target_id = parts[1]
            try:
                delta = int(parts[2])
            except ValueError:
                print("delta must be an integer")
                continue
            await send_cmd(ws, client_id, "SET_HP", {"target_id": target_id, "delta": delta})
            continue

        print("Unknown command. Try /join, /chat, /roll, /hp, /quit")


async def main() -> None:
    # Usage: python client.py ws://localhost:8765 player-123
    uri = "ws://localhost:8765"
    client_id = f"player-{uuid.uuid4().hex[:6]}"

    if len(sys.argv) >= 2:
        uri = sys.argv[1]
    if len(sys.argv) >= 3:
        client_id = sys.argv[2]

    state = ClientState()

    print(f"Connecting to {uri} as {client_id} ...")
    async with websockets.connect(uri) as ws:
        recv_task = asyncio.create_task(receiver(ws, state))
        input_task = asyncio.create_task(user_input_loop(ws, client_id, state))

        done, pending = await asyncio.wait(
            {recv_task, input_task}, return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
