import subprocess
import sys
import time
from pathlib import Path
import os


def start_process(cmd, cwd, stdout_file=None):
    stdout_target = subprocess.PIPE if stdout_file is None else open(stdout_file, "w")
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=stdout_target,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def send_command(proc, command, delay=1.0):
    if proc.stdin is None:
        return
    print(f"Sending: {command}")
    proc.stdin.write(command + "\n")
    proc.stdin.flush()
    time.sleep(delay)


def safe_terminate(proc, name):
    if proc.poll() is None:
        print(f"Stopping {name}...")
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def main():
    os.system("lsof -ti :8765 | xargs kill -9 2>/dev/null")
    project_dir = Path(__file__).resolve().parent

    venv_python = project_dir / ".venv" / "bin" / "python3"
    python_cmd = str(venv_python) if venv_python.exists() else sys.executable

    server_path = project_dir / "server.py"
    client_path = project_dir / "client.py"

    if not server_path.exists():
        print("Error: server.py not found")
        sys.exit(1)

    if not client_path.exists():
        print("Error: client.py not found")
        sys.exit(1)

    print("Starting server...")
    server = start_process(
        [python_cmd, "-u","server.py"],
        cwd=project_dir,
        stdout_file="server_output.log",
    )

    time.sleep(2)

    print("Starting client 1...")
    client1 = start_process(
        [python_cmd, "client.py", "ws://localhost:8765", "player-123"],
        cwd=project_dir,
        stdout_file="client1_output.log",
    )

    time.sleep(1)

    print("Starting client 2...")
    client2 = start_process(
        [python_cmd, "client.py", "ws://localhost:8765", "player-456"],
        cwd=project_dir,
        stdout_file="client2_output.log",
    )

    time.sleep(2)

    try:
        print("\nRunning automated demo sequence...\n")

        send_command(client1, "/join Alice", delay=1.0)
        send_command(client2, "/join Bob", delay=1.0)

        send_command(client1, "/chat hello", delay=1.0)
        send_command(client2, "/roll 20", delay=1.0)

        send_command(client2, "/hp player-123 -5", delay=1.0)

        send_command(client1, "/chat testing ordering", delay=0.7)
        send_command(client2, "/roll 6", delay=0.7)
        send_command(client2, "/chat done", delay=1.0)

        print("\nDemo complete.")
        print("Logs written to:")
        print("  server_output.log")
        print("  client1_output.log")
        print("  client2_output.log")

        time.sleep(3)

    finally:
        print("\nTerminating processes...")
        # safe_terminate(client1, "client 1")
        # safe_terminate(client2, "client 2")
        # safe_terminate(server, "server")


if __name__ == "__main__":
    main()