from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    repo_root: Path
    runtime_dir: Path
    db_dir: Path
    logs_dir: Path
    reports_dir: Path
    snapshots_dir: Path
    db_path: Path
    lock_path: Path
    log_path: Path


def build_runtime_paths(repo_root: Path, runtime_dir_name: str = "runtime") -> RuntimePaths:
    runtime_dir = repo_root / runtime_dir_name
    db_dir = runtime_dir / "db"
    logs_dir = runtime_dir / "logs"
    reports_dir = runtime_dir / "reports"
    snapshots_dir = runtime_dir / "snapshots"

    for path in (runtime_dir, db_dir, logs_dir, reports_dir, snapshots_dir):
        path.mkdir(parents=True, exist_ok=True)

    return RuntimePaths(
        repo_root=repo_root,
        runtime_dir=runtime_dir,
        db_dir=db_dir,
        logs_dir=logs_dir,
        reports_dir=reports_dir,
        snapshots_dir=snapshots_dir,
        db_path=db_dir / "bankupdate.sqlite3",
        lock_path=db_dir / "bankupdate.lock",
        log_path=logs_dir / "bankupdate.log",
    )
