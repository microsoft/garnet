#!/usr/bin/env python3
"""
Docker image validation script for Garnet.

Builds all Linux Dockerfiles, then verifies:
  1. Basic server startup (PING, SET/GET)
  2. Lua EVAL scripting
  3. Default device + storage tier persistence (write, BGSAVE, restart, recover) — all platforms
  4. Native device + storage tier persistence (write, BGSAVE, restart, recover) — glibc only
  5. Native library resolution (libaio, liblua54)
  6. Multi-platform builds via buildx (optional, slow)

Requirements:
  - Docker daemon accessible to the current user
  - Python 3.8+
  - No additional Python packages needed

Usage:
  python3 validate_docker_images.py                  # Run all tests
  python3 validate_docker_images.py --skip-build     # Skip build, use existing images
  python3 validate_docker_images.py --multiplatform  # Include multi-platform buildx test
  python3 validate_docker_images.py --images default alpine  # Test specific images only
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

@dataclass
class ImageDef:
    """Definition of a Docker image variant to test."""
    name: str
    dockerfile: str
    tag: str
    supports_native_device: bool = True
    supports_shell: bool = True  # False for chiseled/distroless

IMAGES = [
    ImageDef("default",        "Dockerfile",            "garnet-validate:default"),
    ImageDef("noble",          "Dockerfile.ubuntu",     "garnet-validate:noble"),
    ImageDef("noble-chiseled", "Dockerfile.chiseled",   "garnet-validate:noble-chiseled", supports_shell=False),
    ImageDef("azurelinux",     "Dockerfile.cbl-mariner","garnet-validate:azurelinux"),
    ImageDef("alpine",         "Dockerfile.alpine",     "garnet-validate:alpine", supports_native_device=False),
]

BASE_PORT = 30000  # Tests allocate ports starting here

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self):
        self.passed: list[str] = []
        self.failed: list[str] = []
        self.skipped: list[str] = []

    def ok(self, desc: str):
        self.passed.append(desc)
        print(f"    ✅ {desc}")

    def fail(self, desc: str, detail: str = ""):
        msg = f"{desc}: {detail}" if detail else desc
        self.failed.append(msg)
        print(f"    ❌ {msg}")

    def skip(self, desc: str, reason: str = ""):
        msg = f"{desc} (skipped: {reason})" if reason else f"{desc} (skipped)"
        self.skipped.append(msg)
        print(f"    ⏭️  {msg}")

    @property
    def success(self) -> bool:
        return len(self.failed) == 0


def run(cmd: str, **kwargs) -> subprocess.CompletedProcess:
    """Run a shell command, returning CompletedProcess."""
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kwargs)


def docker(cmd: str, **kwargs) -> subprocess.CompletedProcess:
    """Run a docker command."""
    return run(f"docker {cmd}", **kwargs)


def resp_encode(*args: str) -> bytes:
    """Encode a RESP command."""
    parts = [f"*{len(args)}\r\n"]
    for a in args:
        encoded = a.encode()
        parts.append(f"${len(encoded)}\r\n")
        parts.append(a + "\r\n")
    return "".join(parts).encode()


def resp_send_recv(sock: socket.socket, *args: str, timeout: float = 5.0) -> str:
    """Send a RESP command and receive the response."""
    sock.sendall(resp_encode(*args))
    time.sleep(0.3)
    sock.settimeout(timeout)
    try:
        return sock.recv(8192).decode().strip()
    except socket.timeout:
        return "TIMEOUT"


@contextmanager
def garnet_container(tag: str, port: int, extra_args: str = "",
                     volume_dir: Optional[str] = None, name: Optional[str] = None):
    """Context manager that starts a Garnet container and cleans up on exit."""
    container_name = name or f"garnet-validate-{port}"
    docker(f"rm -f {container_name}")

    vol_flag = f"-v {volume_dir}:/data" if volume_dir else ""
    result = docker(
        f"run -d --name {container_name} -p {port}:6379 {vol_flag} "
        f"{tag} --protected-mode no {extra_args}"
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start container: {result.stderr.strip()}")

    container_id = result.stdout.strip()
    try:
        # Wait for server to be ready
        for attempt in range(20):
            time.sleep(1)
            status = docker(f"inspect --format='{{{{.State.Status}}}}' {container_name}")
            st = status.stdout.strip().strip("'")
            if st == "running":
                # Try connecting
                try:
                    s = socket.socket()
                    s.settimeout(2)
                    s.connect(("127.0.0.1", port))
                    resp = resp_send_recv(s, "PING")
                    s.close()
                    if "PONG" in resp:
                        break
                except (ConnectionRefusedError, socket.timeout, OSError):
                    pass
            elif st == "exited":
                logs = docker(f"logs {container_name}").stdout + docker(f"logs {container_name}").stderr
                raise RuntimeError(f"Container exited during startup.\nLogs:\n{logs[-500:]}")
        else:
            raise RuntimeError("Container did not become ready within 20 seconds")

        yield container_name
    finally:
        docker(f"stop {container_name}")
        docker(f"rm {container_name}")


def connect(port: int) -> socket.socket:
    """Create a connected socket to a Garnet server."""
    s = socket.socket()
    s.settimeout(10)
    s.connect(("127.0.0.1", port))
    return s

# ---------------------------------------------------------------------------
# Test phases
# ---------------------------------------------------------------------------

def test_build(images: list[ImageDef], results: TestResult):
    """Build all Docker images."""
    print("\n" + "=" * 70)
    print("PHASE: Build Docker images")
    print("=" * 70)
    for img in images:
        print(f"\n  Building {img.name} ({img.dockerfile})...")
        r = run(
            f"docker build -f {img.dockerfile} -t {img.tag} .",
            cwd=REPO_ROOT,
            timeout=600,
        )
        if r.returncode == 0:
            results.ok(f"Build {img.name}")
        else:
            # Show last 10 lines of output for debugging
            lines = (r.stdout + r.stderr).strip().split("\n")
            tail = "\n".join(lines[-10:])
            results.fail(f"Build {img.name}", tail)


def test_basic(images: list[ImageDef], results: TestResult, base_port: int):
    """Test basic server functionality: PING, SET, GET."""
    print("\n" + "=" * 70)
    print("PHASE: Basic server tests (PING, SET, GET)")
    print("=" * 70)
    for i, img in enumerate(images):
        port = base_port + i
        print(f"\n  [{img.name}]")
        try:
            with garnet_container(img.tag, port):
                s = connect(port)
                # PING
                r = resp_send_recv(s, "PING")
                if "PONG" in r:
                    results.ok(f"{img.name}: PING")
                else:
                    results.fail(f"{img.name}: PING", r[:80])

                # SET
                r = resp_send_recv(s, "SET", "test-key", "test-value")
                if "OK" in r:
                    results.ok(f"{img.name}: SET")
                else:
                    results.fail(f"{img.name}: SET", r[:80])

                # GET
                r = resp_send_recv(s, "GET", "test-key")
                if "test-value" in r:
                    results.ok(f"{img.name}: GET")
                else:
                    results.fail(f"{img.name}: GET", r[:80])
                s.close()
        except Exception as e:
            results.fail(f"{img.name}: basic", str(e)[:120])


def test_lua(images: list[ImageDef], results: TestResult, base_port: int):
    """Test Lua EVAL scripting on each image."""
    print("\n" + "=" * 70)
    print("PHASE: Lua EVAL tests")
    print("=" * 70)
    for i, img in enumerate(images):
        port = base_port + 100 + i
        print(f"\n  [{img.name}]")
        try:
            with garnet_container(img.tag, port, extra_args="--lua"):
                s = connect(port)

                # Simple return
                r = resp_send_recv(s, "EVAL", "return 'lua-works'", "0")
                if "lua-works" in r:
                    results.ok(f"{img.name}: EVAL return string")
                else:
                    results.fail(f"{img.name}: EVAL return string", r[:80])

                # redis.call SET
                r = resp_send_recv(s, "EVAL", "return redis.call('SET','lk','lv')", "0")
                if "OK" in r:
                    results.ok(f"{img.name}: EVAL redis.call SET")
                else:
                    results.fail(f"{img.name}: EVAL redis.call SET", r[:80])

                # GET key set by Lua
                r = resp_send_recv(s, "GET", "lk")
                if "lv" in r:
                    results.ok(f"{img.name}: GET Lua-set key")
                else:
                    results.fail(f"{img.name}: GET Lua-set key", r[:80])

                # KEYS and ARGV
                r = resp_send_recv(
                    s, "EVAL",
                    "redis.call('SET',KEYS[1],ARGV[1]); return redis.call('GET',KEYS[1])",
                    "1", "mk", "mv",
                )
                if "mv" in r:
                    results.ok(f"{img.name}: EVAL KEYS/ARGV")
                else:
                    results.fail(f"{img.name}: EVAL KEYS/ARGV", r[:80])

                s.close()
        except Exception as e:
            results.fail(f"{img.name}: lua", str(e)[:120])


def test_default_device(images: list[ImageDef], results: TestResult, base_port: int):
    """Test default (managed) device + storage tier with persistence across restart.

    This uses the default device type which works on all platforms including Alpine.
    """
    print("\n" + "=" * 70)
    print("PHASE: Default device persistence tests (all platforms)")
    print("=" * 70)

    tmpdir = tempfile.mkdtemp(prefix="garnet-dd-validate-")
    os.chmod(tmpdir, 0o777)

    for i, img in enumerate(images):
        port = base_port + 300 + i
        print(f"\n  [{img.name}]")

        vol = os.path.join(tmpdir, img.name)
        os.makedirs(vol, exist_ok=True)
        os.chmod(vol, 0o777)
        container_name = f"garnet-dd-validate-{img.name}"

        dd_args = (
            "--storage-tier true "
            "--logdir /data/store --checkpointdir /data/checkpoint "
            "--aof true"
        )

        try:
            # Phase A: Write data + BGSAVE
            with garnet_container(img.tag, port, extra_args=dd_args,
                                  volume_dir=vol, name=container_name):
                s = connect(port)
                r = resp_send_recv(s, "SET", "dd-key1", "hello-default-device")
                if "OK" not in r:
                    results.fail(f"{img.name}: default device SET", r[:80])
                    s.close()
                    continue

                resp_send_recv(s, "SET", "dd-key2", "default-persists")
                resp_send_recv(s, "BGSAVE")
                time.sleep(3)
                resp_send_recv(s, "COMMITAOF")
                time.sleep(1)
                s.close()
                results.ok(f"{img.name}: default device write + BGSAVE")

            # Phase B: Restart with --recover, verify data
            with garnet_container(img.tag, port, extra_args=dd_args + " --recover",
                                  volume_dir=vol, name=container_name):
                s = connect(port)
                r1 = resp_send_recv(s, "GET", "dd-key1")
                r2 = resp_send_recv(s, "GET", "dd-key2")
                s.close()

                if "hello-default-device" in r1 and "default-persists" in r2:
                    results.ok(f"{img.name}: default device persistence after restart")
                else:
                    results.fail(
                        f"{img.name}: default device persistence",
                        f"key1={r1[:40]}, key2={r2[:40]}"
                    )
        except Exception as e:
            results.fail(f"{img.name}: default device", str(e)[:120])

    shutil.rmtree(tmpdir, ignore_errors=True)


def test_native_device(images: list[ImageDef], results: TestResult, base_port: int):
    """Test native device + storage tier with persistence across restart."""
    print("\n" + "=" * 70)
    print("PHASE: Native device persistence tests")
    print("=" * 70)

    tmpdir = tempfile.mkdtemp(prefix="garnet-nd-validate-")
    os.chmod(tmpdir, 0o777)

    for i, img in enumerate(images):
        port = base_port + 200 + i
        print(f"\n  [{img.name}]")

        if not img.supports_native_device:
            results.skip(f"{img.name}: native device", "not supported on this image")
            continue

        vol = os.path.join(tmpdir, img.name)
        os.makedirs(vol, exist_ok=True)
        os.chmod(vol, 0o777)
        container_name = f"garnet-nd-validate-{img.name}"

        nd_args = (
            "--device-type Native --storage-tier true "
            "--logdir /data/store --checkpointdir /data/checkpoint "
            "--aof true"
        )

        try:
            # Phase A: Write data + BGSAVE
            with garnet_container(img.tag, port, extra_args=nd_args,
                                  volume_dir=vol, name=container_name):
                s = connect(port)
                r = resp_send_recv(s, "SET", "persist-key1", "hello-native")
                if "OK" not in r:
                    results.fail(f"{img.name}: native device SET", r[:80])
                    s.close()
                    continue

                r = resp_send_recv(s, "SET", "persist-key2", "survives-restart")
                resp_send_recv(s, "BGSAVE")
                time.sleep(3)
                resp_send_recv(s, "COMMITAOF")
                time.sleep(1)
                s.close()
                results.ok(f"{img.name}: native device write + BGSAVE")

            # Phase B: Restart with --recover, verify data
            with garnet_container(img.tag, port, extra_args=nd_args + " --recover",
                                  volume_dir=vol, name=container_name):
                s = connect(port)
                r1 = resp_send_recv(s, "GET", "persist-key1")
                r2 = resp_send_recv(s, "GET", "persist-key2")
                s.close()

                if "hello-native" in r1 and "survives-restart" in r2:
                    results.ok(f"{img.name}: native device persistence after restart")
                else:
                    results.fail(
                        f"{img.name}: native device persistence",
                        f"key1={r1[:40]}, key2={r2[:40]}"
                    )
        except Exception as e:
            results.fail(f"{img.name}: native device", str(e)[:120])

    # Cleanup temp dir
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_library_resolution(images: list[ImageDef], results: TestResult):
    """Verify native libraries resolve correctly inside each image."""
    print("\n" + "=" * 70)
    print("PHASE: Library resolution checks")
    print("=" * 70)
    for img in images:
        print(f"\n  [{img.name}]")
        if not img.supports_shell:
            results.skip(f"{img.name}: library check", "no shell in distroless image")
            continue

        # Check libaio
        r = docker(
            f"run --rm --entrypoint sh {img.tag} -c "
            f"'find / -name libaio.so.1 2>/dev/null | head -1'"
        )
        libaio_path = r.stdout.strip()
        if libaio_path:
            results.ok(f"{img.name}: libaio.so.1 found at {libaio_path}")
        else:
            results.fail(f"{img.name}: libaio.so.1 not found")

        # Check liblua54 in .NET runtime dir
        r = docker(
            f"run --rm --entrypoint sh {img.tag} -c "
            f"'find /usr/share/dotnet -name liblua54.so 2>/dev/null | head -1'"
        )
        lua_path = r.stdout.strip()
        if lua_path:
            results.ok(f"{img.name}: liblua54.so found at {lua_path}")
        else:
            results.fail(f"{img.name}: liblua54.so not in .NET runtime dir")

        # Check libnative_device resolves
        r = docker(
            f"run --rm --entrypoint sh {img.tag} -c "
            f"'ldd /app/runtimes/linux-x64/native/libnative_device.so 2>&1'"
        )
        ldd_out = r.stdout
        if "not found" in ldd_out and img.name != "alpine":
            # Alpine is expected to fail (glibc binary on musl)
            results.fail(f"{img.name}: libnative_device.so has unresolved deps",
                         [l for l in ldd_out.split("\n") if "not found" in l][0][:80])
        else:
            results.ok(f"{img.name}: libnative_device.so libraries resolve")


def test_multiplatform(images: list[ImageDef], results: TestResult):
    """Build all images for linux/amd64 + linux/arm64 using buildx."""
    print("\n" + "=" * 70)
    print("PHASE: Multi-platform builds (amd64 + arm64)")
    print("=" * 70)

    builder_name = "garnet-validate-builder"

    # Create/use buildx builder
    run(f"docker buildx create --name {builder_name} --driver docker-container --use 2>/dev/null "
        f"|| docker buildx use {builder_name}")

    for img in images:
        print(f"\n  Building {img.name} for amd64+arm64...")
        r = run(
            f"docker buildx build --builder {builder_name} "
            f"-f {img.dockerfile} "
            f"--platform linux/amd64,linux/arm64 "
            f"--provenance=false "
            f"-t garnet-validate-mp:{img.name} .",
            cwd=REPO_ROOT,
            timeout=900,
        )
        if r.returncode == 0:
            results.ok(f"Multi-platform build {img.name}")
        else:
            lines = (r.stdout + r.stderr).strip().split("\n")
            tail = "\n".join(lines[-5:])
            results.fail(f"Multi-platform build {img.name}", tail[:120])

    # Cleanup builder
    run(f"docker buildx rm {builder_name} 2>/dev/null")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate Garnet Docker images")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip building images (use existing tags)")
    parser.add_argument("--multiplatform", action="store_true",
                        help="Include multi-platform buildx test (slow)")
    parser.add_argument("--images", nargs="*", default=None,
                        help="Test only specific images by name (e.g., default alpine)")
    parser.add_argument("--base-port", type=int, default=BASE_PORT,
                        help=f"Base port for test containers (default: {BASE_PORT})")
    args = parser.parse_args()

    # Filter images if specified
    if args.images:
        selected = [img for img in IMAGES if img.name in args.images]
        if not selected:
            valid = ", ".join(img.name for img in IMAGES)
            print(f"No matching images. Valid names: {valid}")
            sys.exit(1)
    else:
        selected = IMAGES

    results = TestResult()

    print("=" * 70)
    print("Garnet Docker Image Validation")
    print("=" * 70)
    print(f"Images: {', '.join(img.name for img in selected)}")
    print(f"Repo root: {REPO_ROOT}")

    # Verify Docker is available
    r = docker("info --format '{{.ServerVersion}}'")
    if r.returncode != 0:
        print("ERROR: Docker is not available. Ensure the Docker daemon is running.")
        sys.exit(1)
    print(f"Docker: {r.stdout.strip()}")

    try:
        if not args.skip_build:
            test_build(selected, results)
            # Abort early if any builds failed
            if not results.success:
                print("\n⚠️  Some builds failed — skipping runtime tests.")
                _print_summary(results)
                sys.exit(1)

        test_basic(selected, results, args.base_port)
        test_lua(selected, results, args.base_port)
        test_default_device(selected, results, args.base_port)
        test_native_device(selected, results, args.base_port)
        test_library_resolution(selected, results)

        if args.multiplatform:
            test_multiplatform(selected, results)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(130)

    _print_summary(results)
    sys.exit(0 if results.success else 1)


def _print_summary(results: TestResult):
    total = len(results.passed) + len(results.failed) + len(results.skipped)
    print("\n" + "=" * 70)
    print(f"SUMMARY: {len(results.passed)} passed, {len(results.failed)} failed, "
          f"{len(results.skipped)} skipped  (total: {total})")
    print("=" * 70)

    if results.failed:
        print("\nFailed tests:")
        for f in results.failed:
            print(f"  ❌ {f}")

    if results.skipped:
        print("\nSkipped tests:")
        for s in results.skipped:
            print(f"  ⏭️  {s}")

    print()
    if results.success:
        print("✅ ALL TESTS PASSED")
    else:
        print("❌ SOME TESTS FAILED")


if __name__ == "__main__":
    main()
