import socket
import subprocess
import time
import uuid
from collections.abc import Iterator

import pytest
from redis import Redis


def _find_open_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    port = _find_open_port()
    container_name = f"escrowmint-py-test-{uuid.uuid4().hex[:8]}"
    image = "redis:7-alpine"

    run = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container_name,
            "-p",
            f"{port}:6379",
            image,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if run.returncode != 0:
        pytest.skip(f"unable to start Redis test container: {run.stderr.strip()}")

    client = Redis(host="127.0.0.1", port=port, decode_responses=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if client.ping():
                break
        except Exception:
            time.sleep(0.25)
    else:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
        pytest.fail("Redis test container did not become ready in time")

    try:
        yield f"redis://127.0.0.1:{port}/0"
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
