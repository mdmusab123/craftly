#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "websockets"])
        import websockets  # type: ignore[no-redef]
    except Exception:
        raise SystemExit(
            "Missing dependency: websockets\n"
            "Automatic install failed. Run: python3 -m pip install --user websockets"
        ) from exc


CHUNK_SIZE = 256 * 1024
PING_INTERVAL = None
PING_TIMEOUT = None
DEFAULT_SERVER_URL = "wss://web3.craftlyrobot.com/"
DEFAULT_SHARED_TOKEN = "a792db2ce7d54e715acf0d87351bf5d2"
DEFAULT_WORKSPACE = str(Path.home() / "Documents" / "Craftly-Worker")
ACTIVE_COMMAND_RAM_BYTES = 1024 * 1024 * 1024
RESOURCE_FLAG_PREFIXES = ("--cpu", "--ram", "--gpu")
DEBUG_LIST_LIMIT = 200


def utc_ts() -> int:
    return int(time.time())


def jdump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"))


def jload(text: str) -> dict[str, Any]:
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("message must decode to an object")
    return data


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_worker_id() -> str:
    hostname = socket.gethostname().strip() or "worker"
    username = (
        os.environ.get("CRAFTLY_WORKER_USER")
        or os.environ.get("USER")
        or os.environ.get("USERNAME")
        or ""
    ).strip()
    machine_bits = "|".join(
        [
            hostname,
            username,
            platform.system(),
            platform.machine(),
            hex(uuid.getnode()),
        ]
    )
    suffix = hashlib.sha256(machine_bits.encode("utf-8", errors="replace")).hexdigest()[:8]
    return f"{hostname}-{suffix}"


def _candidate_tool_paths(binary: str) -> list[Path]:
    candidates: list[Path] = []
    if os.name == "nt":
        names = [binary] if binary.lower().endswith(".exe") else [binary, f"{binary}.exe"]
        roots = [
            Path("C:/Program Files"),
            Path("C:/Program Files (x86)"),
            Path.home() / "scoop" / "shims",
            Path.home() / "AppData/Local/Microsoft/WinGet/Packages",
            Path.home() / "AppData/Local/Programs",
        ]
        for root in roots:
            for name in names:
                candidates.extend(
                    [
                        root / name,
                        root / "Flutter" / "bin" / name,
                        root / "nodejs" / name,
                    ]
                )
    else:
        candidates.extend(
            [
                Path("/opt/homebrew/bin") / binary,
                Path("/usr/local/bin") / binary,
                Path("/opt/local/bin") / binary,
                Path.home() / ".local/bin" / binary,
                Path.home() / "flutter/bin" / binary,
            ]
        )
    return candidates


def resolve_tool_path(binary: str) -> str:
    resolved = shutil.which(binary)
    if resolved:
        return resolved
    for candidate in _candidate_tool_paths(binary):
        if candidate.exists():
            return str(candidate)
    return ""


def build_subprocess_env(argv0: str = "") -> dict[str, str]:
    env = os.environ.copy()
    path_entries: list[str] = []
    existing = str(env.get("PATH", "")).strip()
    if existing:
        path_entries.extend(entry for entry in existing.split(os.pathsep) if entry)
    if argv0:
        tool_dir = str(Path(argv0).expanduser().resolve().parent)
        if tool_dir:
            path_entries.insert(0, tool_dir)
    if os.name == "nt":
        path_entries.extend(
            [
                str(Path.home() / "scoop" / "shims"),
                "C:\\Program Files\\nodejs",
                str(Path.home() / "AppData/Local/Programs/nodejs"),
                str(Path.home() / "AppData/Local/Microsoft/WinGet/Packages"),
            ]
        )
    else:
        path_entries.extend(
            [
                "/opt/homebrew/bin",
                "/usr/local/bin",
                "/opt/local/bin",
                str(Path.home() / ".local/bin"),
                str(Path.home() / "flutter/bin"),
            ]
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in path_entries:
        value = str(entry).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    env["PATH"] = os.pathsep.join(normalized)
    return env


def detect_gpu_info() -> tuple[int, list[str]]:
    try:
        if sys.platform == "darwin":
            proc = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                data = json.loads(proc.stdout)
                items = data.get("SPDisplaysDataType", []) or []
                names = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    name = str(
                        item.get("sppci_model")
                        or item.get("_name")
                        or item.get("spdisplays_vendor")
                        or ""
                    ).strip()
                    if name:
                        names.append(name)
                if names:
                    return len(names), names
        if os.name == "nt":
            proc = subprocess.run(
                ["wmic", "path", "win32_VideoController", "get", "name"],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0:
                names = [line.strip() for line in proc.stdout.splitlines() if line.strip() and line.strip().lower() != "name"]
                if names:
                    return len(names), names
        nvidia = resolve_tool_path("nvidia-smi")
        if nvidia:
            proc = subprocess.run([nvidia, "-L"], capture_output=True, text=True, timeout=8)
            if proc.returncode == 0:
                names = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
                if names:
                    return len(names), names
        if sys.platform.startswith("linux"):
            proc = subprocess.run(
                ["sh", "-lc", "lspci | grep -Ei 'vga|3d|display'"],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode == 0:
                names = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
                if names:
                    return len(names), names
    except Exception:
        return 0, []
    return 0, []


def suggest_validator_jobs(cpu_count: int | None, memory_bytes: int | None) -> int:
    cpu_total = max(1, int(cpu_count or 1))
    if cpu_total >= 8:
        cpu_cap = cpu_total - 2
    elif cpu_total >= 4:
        cpu_cap = cpu_total - 1
    else:
        cpu_cap = cpu_total
    if memory_bytes and memory_bytes > 0:
        mem_cap = max(1, int(memory_bytes // (3 * 1024 * 1024 * 1024)))
    else:
        mem_cap = cpu_cap
    return max(1, min(cpu_cap, mem_cap, 8))


def format_ram(memory_bytes: int | None) -> str:
    if memory_bytes is None:
        return "unknown"
    gib = memory_bytes / (1024 ** 3)
    return f"{gib:.1f} GB"


def contribution_ram_bytes(memory_bytes: int | None, validator_jobs: int) -> int | None:
    if not memory_bytes:
        return None
    reserved = validator_jobs * 3 * 1024 * 1024 * 1024
    return min(memory_bytes, reserved)


def normalize_resource_flag_args(argv: list[str]) -> list[str]:
    normalized: list[str] = []
    for arg in argv:
        matched = False
        for prefix in RESOURCE_FLAG_PREFIXES:
            if arg.startswith(prefix) and arg != prefix:
                suffix = arg[len(prefix):]
                if suffix.isdigit():
                    normalized.extend([prefix, suffix])
                    matched = True
                    break
        if not matched:
            normalized.append(arg)
    return normalized


def clamp_positive_limit(value: int | None, cap: int | None) -> int | None:
    if value is None:
        return cap
    if cap is None:
        return value
    return max(0, min(int(value), int(cap)))


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be at least 0")
    return parsed


def run_tool_probe(argv: list[str], *, timeout: int = 12) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=build_subprocess_env(argv[0]),
        )
    except Exception as exc:
        return False, str(exc)
    output = (proc.stdout or "").strip() or (proc.stderr or "").strip()
    if proc.returncode != 0:
        return False, output or f"exit_{proc.returncode}"
    return True, (output.splitlines()[0] if output else "ok")


def validate_toolchain() -> dict[str, Any]:
    node_path = resolve_tool_path("node")
    npm_path = resolve_tool_path("npm")
    flutter_path = resolve_tool_path("flutter")
    checks = {
        "NodeJS": {
            "path": node_path,
            "command": [node_path, "--version"] if node_path else [],
            "install_hint": "Install Node.js from https://nodejs.org/ or your package manager.",
        },
        "npm": {
            "path": npm_path,
            "command": [npm_path, "--version"] if npm_path else [],
            "install_hint": "Install npm alongside Node.js.",
        },
        "Flutter": {
            "path": flutter_path,
            "command": [flutter_path, "--version"] if flutter_path else [],
            "install_hint": "Install Flutter from https://docs.flutter.dev/get-started/install .",
        },
    }
    missing: list[str] = []
    broken: list[str] = []
    notes: dict[str, str] = {}
    versions: dict[str, str] = {}
    for label, meta in checks.items():
        path = str(meta["path"] or "")
        if not path:
            missing.append(label)
            notes[label] = str(meta["install_hint"])
            continue
        ok, detail = run_tool_probe(list(meta["command"]))
        if not ok:
            broken.append(label)
            notes[label] = detail
            continue
        versions[label] = detail
    return {
        "node_path": node_path,
        "npm_path": npm_path,
        "flutter_path": flutter_path,
        "toolchain_ready": not missing and not broken,
        "missing_tools": missing,
        "broken_tools": broken,
        "tool_versions": versions,
        "tool_notes": notes,
    }


def print_toolchain_rejection(info: dict[str, Any]) -> None:
    print("Craftly Central Orchestrator Connection [BLOCKED]")
    print("Hardware's Contribution")
    print(f"CPU [{info.get('contribution_cpu_slots')}]")
    print(f"GPU [{info.get('contribution_gpu_count')}]")
    print(f"RAM [{format_ram(info.get('contribution_ram_bytes'))}]")
    print("")
    print("Toolchain Check")
    for label, path_key in (("NodeJS", "node_path"), ("npm", "npm_path"), ("Flutter", "flutter_path")):
        path_value = str(info.get(path_key) or "").strip()
        if not path_value:
            print(f"{label} [MISSING]")
        elif label in set(info.get("broken_tools") or []):
            print(f"{label} [BROKEN]")
    print("")
    print("Worker Status [RED FLAG]")
    for label in list(info.get("missing_tools") or []) + list(info.get("broken_tools") or []):
        note = str((info.get("tool_notes") or {}).get(label, "")).strip()
        if note:
            print(f"{label}: {note}")
    print("Please install the missing requirements and run `python worker.py` again.")


def detect_total_memory_bytes() -> int | None:
    if sys.platform == "darwin":
        try:
            value = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
            return int(value)
        except Exception:
            return None
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
        except Exception:
            return None
    if os.name == "nt":
        try:
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [
                    ("length", ctypes.c_uint32),
                    ("memoryLoad", ctypes.c_uint32),
                    ("totalPhys", ctypes.c_uint64),
                    ("availPhys", ctypes.c_uint64),
                    ("totalPageFile", ctypes.c_uint64),
                    ("availPageFile", ctypes.c_uint64),
                    ("totalVirtual", ctypes.c_uint64),
                    ("availVirtual", ctypes.c_uint64),
                    ("availExtendedVirtual", ctypes.c_uint64),
                ]

            status = MemoryStatus()
            status.length = ctypes.sizeof(MemoryStatus)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
            return int(status.totalPhys)
        except Exception:
            return None
    return None


def zip_directory(source_dir: Path, output_zip: Path) -> tuple[int, int]:
    file_count = 0
    total_bytes = 0
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(source_dir)
            archive.write(path, arcname=str(rel))
            file_count += 1
            total_bytes += path.stat().st_size
    return file_count, total_bytes


def list_tree_files(source_dir: Path, *, limit: int = DEBUG_LIST_LIMIT) -> list[dict[str, Any]]:
    if not source_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
            rows.append(
                {
                    "relative_path": str(path.relative_to(source_dir)),
                    "size": stat.st_size,
                    "sha256": sha256_file(path),
                }
            )
        except Exception as exc:
            rows.append({"relative_path": str(path), "error": str(exc)})
        if len(rows) >= limit:
            break
    return rows


def list_zip_entries(zip_path: Path, *, limit: int = DEBUG_LIST_LIMIT) -> list[dict[str, Any]]:
    if not zip_path.exists():
        return []
    entries: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(zip_path) as archive:
            for info in archive.infolist()[:limit]:
                entries.append({"name": info.filename, "size": info.file_size})
    except Exception as exc:
        entries.append({"error": str(exc)})
    return entries


def collect_run_debug(
    *,
    run_root: Path,
    job_id: str,
    worker_id: str,
    info: dict[str, Any],
    command: list[str],
    env: dict[str, str],
    input_dir: Path,
    output_dir: Path,
    artifacts_dir: Path,
    clean_zip: Path | None = None,
    return_code: int | None = None,
    duration_sec: float | None = None,
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "worker_id": worker_id,
        "created_at_unix": utc_ts(),
        "platform": platform.platform(),
        "python_version": sys.version,
        "python_executable": sys.executable,
        "cwd": str(run_root),
        "command": command,
        "return_code": return_code,
        "duration_sec": duration_sec,
        "env_path": env.get("PATH", ""),
        "tool_paths": {
            "node": info.get("node_path"),
            "npm": info.get("npm_path"),
            "flutter": info.get("flutter_path"),
        },
        "tool_versions": dict(info.get("tool_versions") or {}),
        "input_files": list_tree_files(input_dir),
        "output_files": list_tree_files(output_dir),
        "artifact_files": list_tree_files(artifacts_dir),
        "clean_zip": {
            "path": str(clean_zip) if clean_zip is not None else "",
            "exists": clean_zip.exists() if clean_zip is not None else False,
            "entries": list_zip_entries(clean_zip) if clean_zip is not None else [],
        },
    }


async def send_json(websocket: Any, send_lock: asyncio.Lock, payload: dict[str, Any]) -> None:
    async with send_lock:
        await websocket.send(jdump(payload))


async def stream_file(
    websocket: Any,
    send_lock: asyncio.Lock,
    *,
    job_id: str,
    role: str,
    path: Path,
) -> None:
    seq = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            seq += 1
            await send_json(
                websocket,
                send_lock,
                {
                    "type": "file_chunk",
                    "job_id": job_id,
                    "role": role,
                    "seq": seq,
                    "payload_b64": base64.b64encode(chunk).decode("ascii"),
                },
            )
    await send_json(
        websocket,
        send_lock,
        {
            "type": "file_complete",
            "job_id": job_id,
            "role": role,
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        },
    )


def worker_info(worker_id: str, args: argparse.Namespace) -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    memory_bytes = detect_total_memory_bytes()
    gpu_count, gpu_names = detect_gpu_info()
    toolchain = validate_toolchain()
    cpu_limit = clamp_positive_limit(args.cpu, cpu_count)
    gpu_limit = clamp_positive_limit(args.gpu, gpu_count)
    ram_limit_bytes = None
    if args.ram is not None:
        ram_limit_bytes = args.ram * 1024 * 1024 * 1024
    effective_memory_bytes = memory_bytes
    if effective_memory_bytes is not None and ram_limit_bytes is not None:
        effective_memory_bytes = min(effective_memory_bytes, ram_limit_bytes)
    elif effective_memory_bytes is None and ram_limit_bytes is not None:
        effective_memory_bytes = ram_limit_bytes
    validator_jobs = suggest_validator_jobs(cpu_limit or cpu_count, effective_memory_bytes)
    if cpu_limit is not None:
        validator_jobs = min(validator_jobs, cpu_limit)
    reserved_ram = contribution_ram_bytes(effective_memory_bytes, validator_jobs)
    return {
        "worker_id": worker_id,
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "cpu_count": cpu_count,
        "memory_bytes": memory_bytes,
        "gpu_count": gpu_count,
        "gpu_names": gpu_names,
        "validator_jobs": validator_jobs,
        "contribution_cpu_slots": validator_jobs,
        "contribution_gpu_count": gpu_limit if gpu_limit is not None else gpu_count,
        "contribution_ram_bytes": reserved_ram,
        "resource_limits": {
            "cpu": cpu_limit,
            "ram_bytes": effective_memory_bytes,
            "gpu": gpu_limit if gpu_limit is not None else gpu_count,
        },
        "node_path": toolchain["node_path"],
        "npm_path": toolchain["npm_path"],
        "flutter_path": toolchain["flutter_path"],
        "toolchain_ready": bool(toolchain["toolchain_ready"]),
        "missing_tools": list(toolchain["missing_tools"]),
        "broken_tools": list(toolchain["broken_tools"]),
        "tool_versions": dict(toolchain["tool_versions"]),
        "tool_notes": dict(toolchain["tool_notes"]),
    }


def extract_zip(zip_path: Path, output_dir: Path) -> None:
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(output_dir)


async def pump_stream(
    stream: asyncio.StreamReader,
    websocket: Any,
    send_lock: asyncio.Lock,
    *,
    event_type: str,
    entity_id: str,
    stream_name: str,
) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace").rstrip("\n")
        await send_json(
            websocket,
            send_lock,
            {
                "type": event_type,
                "job_id": entity_id,
                "command_id": entity_id,
                "stream": stream_name,
                "line": text,
            },
        )


async def heartbeat_loop(websocket: Any, send_lock: asyncio.Lock, worker_id: str) -> None:
    while True:
        await asyncio.sleep(15)
        try:
            await send_json(
                websocket,
                send_lock,
                {
                    "type": "heartbeat",
                    "worker_id": worker_id,
                    "time_unix": utc_ts(),
                },
            )
        except Exception:
            return


def live_contribution_snapshot(info: dict[str, Any], mode: str) -> dict[str, Any]:
    if mode == "validator":
        return {
            "mode": mode,
            "cpu": int(info.get("contribution_cpu_slots") or 0),
            "gpu": int(info.get("contribution_gpu_count") or 0),
            "ram_bytes": info.get("contribution_ram_bytes"),
        }
    if mode == "command":
        cap_cpu = int(info.get("contribution_cpu_slots") or 0)
        cap_ram = info.get("contribution_ram_bytes")
        ram_bytes = ACTIVE_COMMAND_RAM_BYTES if cap_ram is None else min(int(cap_ram), ACTIVE_COMMAND_RAM_BYTES)
        return {
            "mode": mode,
            "cpu": 1 if cap_cpu > 0 else 0,
            "gpu": 0,
            "ram_bytes": ram_bytes,
        }
    return {
        "mode": "idle",
        "cpu": 0,
        "gpu": 0,
        "ram_bytes": 0,
    }


def print_live_contribution(info: dict[str, Any], mode: str, previous: dict[str, Any] | None) -> dict[str, Any]:
    current = live_contribution_snapshot(info, mode)
    if previous == current:
        return current
    print("")
    print("Actively Contributing")
    print(f"CPU [{current['cpu']}]")
    print(f"GPU [{current['gpu']}]")
    print(f"RAM [{format_ram(current['ram_bytes'])}]")
    return current


async def run_job(
    websocket: Any,
    send_lock: asyncio.Lock,
    workspace: Path,
    python_bin: str,
    worker_id: str,
    info: dict[str, Any],
    job: dict[str, Any],
    files: dict[str, Path],
    keep_runs: bool,
) -> None:
    job_id = str(job["job_id"])
    run_root = ensure_dir(workspace / "runs" / job_id)
    input_dir = ensure_dir(run_root / "input")
    output_dir = ensure_dir(run_root / "output")
    artifacts_dir = ensure_dir(run_root / "output_artifacts")
    extract_zip(files["input_archive"], input_dir)

    validator_file = files["validator_script"]
    validator_args = [str(item) for item in job.get("validator_args", [])]
    if "--jobs" not in validator_args and not any(item.startswith("--jobs=") for item in validator_args):
        validator_args.extend(["--jobs", str(info.get("validator_jobs") or 1)])
    command = [
        python_bin,
        str(validator_file),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--artifacts-dir",
        str(artifacts_dir),
        *validator_args,
    ]

    await send_json(websocket, send_lock, {"type": "job_started", "job_id": job_id, "command": command})
    started = time.time()
    env = build_subprocess_env(command[0])
    debug_path = run_root / "worker_debug.json"
    write_json_file(
        debug_path,
        collect_run_debug(
            run_root=run_root,
            job_id=job_id,
            worker_id=worker_id,
            info=info,
            command=command,
            env=env,
            input_dir=input_dir,
            output_dir=output_dir,
            artifacts_dir=artifacts_dir,
        ),
    )
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(run_root),
        env=env,
    )
    stdout_task = asyncio.create_task(
        pump_stream(process.stdout, websocket, send_lock, event_type="job_log", entity_id=job_id, stream_name="stdout")
    )
    stderr_task = asyncio.create_task(
        pump_stream(process.stderr, websocket, send_lock, event_type="job_log", entity_id=job_id, stream_name="stderr")
    )
    return_code = await process.wait()
    await stdout_task
    await stderr_task

    clean_zip = run_root / f"{job_id}.clean_output.zip"
    clean_file_count, _ = zip_directory(output_dir, clean_zip)
    duration_sec = round(time.time() - started, 3)
    write_json_file(
        debug_path,
        collect_run_debug(
            run_root=run_root,
            job_id=job_id,
            worker_id=worker_id,
            info=info,
            command=command,
            env=env,
            input_dir=input_dir,
            output_dir=output_dir,
            artifacts_dir=artifacts_dir,
            clean_zip=clean_zip,
            return_code=return_code,
            duration_sec=duration_sec,
        ),
    )
    shutil.copy2(debug_path, artifacts_dir / "worker_debug.json")
    clean_meta = {
        "name": clean_zip.name,
        "size": clean_zip.stat().st_size,
        "sha256": sha256_file(clean_zip),
        "file_count": clean_file_count,
    }

    artifacts_meta: dict[str, Any] | None = None
    artifacts_zip: Path | None = None
    artifact_file_count = 0
    if job.get("return_artifacts"):
        artifacts_zip = run_root / f"{job_id}.artifacts.zip"
        artifact_file_count, _ = zip_directory(artifacts_dir, artifacts_zip)
        artifacts_meta = {
            "name": artifacts_zip.name,
            "size": artifacts_zip.stat().st_size,
            "sha256": sha256_file(artifacts_zip),
            "file_count": artifact_file_count,
        }

    summary = {
        "job_id": job_id,
        "worker_id": worker_id,
        "return_code": return_code,
        "duration_sec": duration_sec,
        "clean_file_count": clean_file_count,
        "artifact_file_count": artifact_file_count,
        "input_zip_name": job.get("zip_name"),
        "validator_args": list(job.get("validator_args", [])),
        "worker_debug_path": str(debug_path),
        "worker_run_root": str(run_root),
        "output_files": list_tree_files(output_dir, limit=50),
        "artifact_files": list_tree_files(artifacts_dir, limit=50),
        "status": "completed" if return_code == 0 else "failed",
    }

    await send_json(
        websocket,
        send_lock,
        {
            "type": "job_result",
            "job_id": job_id,
            "clean_output": clean_meta,
            "artifacts": artifacts_meta,
        },
    )
    await stream_file(websocket, send_lock, job_id=job_id, role="clean_output", path=clean_zip)
    if artifacts_zip is not None and artifacts_meta is not None:
        await stream_file(websocket, send_lock, job_id=job_id, role="artifacts", path=artifacts_zip)

    await send_json(
        websocket,
        send_lock,
        {
            "type": "job_finished",
            "job_id": job_id,
            "status": "completed" if return_code == 0 else "failed",
            "error": "" if return_code == 0 else f"validator_exit_{return_code}",
            "summary": summary,
        },
    )
    should_keep_run = keep_runs or job_id.startswith("calib_") or (clean_file_count == 0 and return_code == 0)
    if should_keep_run:
        print(f"Debug kept for {job_id}: {run_root}", file=sys.stderr)
    else:
        shutil.rmtree(run_root, ignore_errors=True)


async def run_shell_command(
    websocket: Any,
    send_lock: asyncio.Lock,
    *,
    command_id: str,
    command_text: str,
    cwd: str,
    shell_hint: str,
) -> None:
    await send_json(
        websocket,
        send_lock,
        {
            "type": "shell_started",
            "command_id": command_id,
            "command": command_text,
        },
    )
    if os.name == "nt":
        shell_argv = [shell_hint or "cmd.exe", "/c", command_text]
    else:
        shell_argv = [shell_hint or os.environ.get("SHELL") or "/bin/zsh", "-lc", command_text]
    env = build_subprocess_env(shell_argv[0])
    started = time.time()
    process = await asyncio.create_subprocess_exec(
        *shell_argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd or None,
        env=env,
    )
    stdout_task = asyncio.create_task(
        pump_stream(process.stdout, websocket, send_lock, event_type="shell_log", entity_id=command_id, stream_name="stdout")
    )
    stderr_task = asyncio.create_task(
        pump_stream(process.stderr, websocket, send_lock, event_type="shell_log", entity_id=command_id, stream_name="stderr")
    )
    return_code = await process.wait()
    await stdout_task
    await stderr_task
    await send_json(
        websocket,
        send_lock,
        {
            "type": "shell_finished",
            "command_id": command_id,
            "success": return_code == 0,
            "summary": {
                "command_id": command_id,
                "return_code": return_code,
                "duration_sec": round(time.time() - started, 3),
                "command": command_text,
                "cwd": cwd,
            },
        },
    )


async def worker_loop(args: argparse.Namespace) -> None:
    workspace = ensure_dir(Path(args.workspace).expanduser().resolve())
    inbox_dir = ensure_dir(workspace / "inbox")
    heartbeat_task: asyncio.Task[None] | None = None
    announced = False
    live_snapshot: dict[str, Any] | None = None
    wid = args.worker_id or default_worker_id()
    info = worker_info(wid, args)
    if not bool(info.get("toolchain_ready")):
        print_toolchain_rejection(info)
        return
    while True:
        try:
            async with websockets.connect(
                args.server,
                max_size=None,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
            ) as websocket:
                send_lock = asyncio.Lock()
                await send_json(
                    websocket,
                    send_lock,
                    {
                        "type": "register",
                        "token": args.token,
                        "worker": info,
                    },
                )
                heartbeat_task = asyncio.create_task(heartbeat_loop(websocket, send_lock, wid))
                pending_job: dict[str, Any] | None = None
                pending_paths: dict[str, Path] = {}
                received_roles: set[str] = set()
                while True:
                    raw = await websocket.recv()
                    message = jload(raw)
                    msg_type = str(message.get("type", ""))
                    if msg_type == "register_ack":
                        if not announced:
                            print("connected to Craftly Central Orchestrator Server")
                            print("")
                            print("Worker Status [GREEN FLAG]")
                            live_snapshot = print_live_contribution(info, "idle", live_snapshot)
                            announced = True
                        await send_json(websocket, send_lock, {"type": "ready", "worker_id": wid})
                        continue
                    if msg_type == "idle":
                        live_snapshot = print_live_contribution(info, "idle", live_snapshot)
                        await asyncio.sleep(int(message.get("retry_after_sec", 10)))
                        await send_json(websocket, send_lock, {"type": "ready", "worker_id": wid})
                        continue
                    if msg_type == "error":
                        error_name = str(message.get("message", "commander_error"))
                        if error_name == "toolchain_invalid":
                            print("Worker Status [RED FLAG]")
                            for label in list(message.get("missing_tools") or []) + list(message.get("broken_tools") or []):
                                print(f"{label} [SERVER REJECTED]")
                            print("Please install the missing requirements and run `python worker.py` again.")
                            return
                        if error_name == "calibration_failed":
                            expected = int(message.get("expected_clean_file_count") or 0)
                            actual = int(message.get("actual_clean_file_count") or 0)
                            print("Worker Status [RED FLAG]")
                            print("This computer's validator runtime does not match Craftly's reference environment.")
                            print(f"Calibration expected clean batches [{expected}] but got [{actual}].")
                            print(f"Debug folder: {workspace / 'runs'}")
                            print("This worker will not be used for cleanup jobs until the environment is fixed.")
                            return
                        raise RuntimeError(error_name)
                    if msg_type == "job_assignment":
                        live_snapshot = print_live_contribution(info, "validator", live_snapshot)
                        pending_job = dict(message["job"])
                        job_id = str(pending_job["job_id"])
                        job_dir = ensure_dir(inbox_dir / job_id)
                        pending_paths = {
                            "validator_script": job_dir / str(pending_job["validator_name"]),
                            "input_archive": job_dir / str(pending_job["zip_name"]),
                        }
                        received_roles = set()
                        for path in pending_paths.values():
                            if path.exists():
                                path.unlink()
                        continue
                    if msg_type == "file_chunk":
                        if pending_job is None:
                            raise ValueError("received file chunk before job assignment")
                        role = str(message["role"])
                        path = pending_paths[role]
                        with path.open("ab") as handle:
                            handle.write(base64.b64decode(message["payload_b64"]))
                        continue
                    if msg_type == "file_complete":
                        if pending_job is None:
                            raise ValueError("received file complete before job assignment")
                        role = str(message["role"])
                        path = pending_paths[role]
                        actual_size = path.stat().st_size
                        actual_sha = sha256_file(path)
                        if actual_size != int(message["size"]):
                            raise ValueError(f"size mismatch for {role}")
                        if actual_sha != str(message["sha256"]):
                            raise ValueError(f"sha256 mismatch for {role}")
                        received_roles.add(role)
                        if received_roles == {"validator_script", "input_archive"}:
                            await run_job(
                                websocket,
                                send_lock,
                                workspace=workspace,
                                python_bin=args.python_bin,
                                worker_id=wid,
                                info=info,
                                job=pending_job,
                                files=pending_paths,
                                keep_runs=bool(args.keep_runs),
                            )
                            live_snapshot = print_live_contribution(info, "idle", live_snapshot)
                            pending_job = None
                            pending_paths = {}
                            received_roles = set()
                            await send_json(websocket, send_lock, {"type": "ready", "worker_id": wid})
                        continue
                    if msg_type == "shell_command":
                        live_snapshot = print_live_contribution(info, "command", live_snapshot)
                        await run_shell_command(
                            websocket,
                            send_lock,
                            command_id=str(message["command_id"]),
                            command_text=str(message["command"]),
                            cwd=str(message.get("cwd", "")),
                            shell_hint=str(message.get("shell", "")),
                        )
                        live_snapshot = print_live_contribution(info, "idle", live_snapshot)
                        await send_json(websocket, send_lock, {"type": "ready", "worker_id": wid})
                        continue
        except KeyboardInterrupt:
            raise
        except websockets.ConnectionClosed:
            print("Connection to Craftly Central Orchestrator Server was interrupted. Reconnecting...", file=sys.stderr)
            await asyncio.sleep(3)
        except Exception as exc:
            print(f"Worker reconnecting after error: {exc}", file=sys.stderr)
            await asyncio.sleep(5)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat_task
                heartbeat_task = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Craftly batch cleanup worker")
    parser.add_argument("--server", default=DEFAULT_SERVER_URL, help="Commander WebSocket URL")
    parser.add_argument("--token", default=DEFAULT_SHARED_TOKEN, help="Shared auth token from the commander")
    parser.add_argument("--workspace", default=DEFAULT_WORKSPACE, help="Worker workspace")
    parser.add_argument("--worker-id", help="Override worker id")
    parser.add_argument("--python-bin", default=sys.executable, help="Python interpreter used to run cleanup_validator.py")
    parser.add_argument("--keep-runs", action="store_true", help="Keep extracted job workspaces after completion")
    parser.add_argument("--cpu", type=positive_int, help="Cap contributed CPU worker slots")
    parser.add_argument("--ram", type=positive_int, help="Cap contributed RAM in GiB")
    parser.add_argument("--gpu", type=nonnegative_int, help="Cap contributed GPU count")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args(normalize_resource_flag_args(sys.argv[1:]))
    try:
        asyncio.run(worker_loop(args))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
