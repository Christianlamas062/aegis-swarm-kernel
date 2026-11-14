"""Build the clean Agensi.io commercial ZIP bundle for `aegis-swarm-kernel`."""

from __future__ import annotations

import zipfile
from pathlib import Path

EXCLUDED_FILES = {
    "MARKETPLACE.md",
    "build_agensi_zip.py",
    ".DS_Store",
}

EXCLUDED_DIRS = {
    ".git",
    ".agent",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "dist",
    "build",
}

EXCLUDED_SUFFIXES = (".pyc", ".pyo", ".db", ".sqlite", ".sqlite3")


def should_exclude(rel_path: Path) -> bool:
    """Return True if a project-relative path should be excluded from the Agensi ZIP."""
    if rel_path.name in EXCLUDED_FILES:
        return True
    if rel_path.as_posix() == "scripts/build_agensi_zip.py":
        return True
    for part in rel_path.parts:
        if part in EXCLUDED_DIRS or part.endswith(".egg-info"):
            return True
    if rel_path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    return False


def build_bundle() -> Path:
    """Package clean deliverable files into `dist/aegis-swarm-kernel-v1.0.0.zip`."""
    project_root = Path(__file__).resolve().parents[1]
    dist_dir = project_root / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    archive_path = dist_dir / "aegis-swarm-kernel-v1.0.0.zip"

    included_files: list[Path] = []
    for candidate in sorted(project_root.rglob("*")):
        if not candidate.is_file():
            continue
        rel_path = candidate.relative_to(project_root)
        if should_exclude(rel_path):
            continue
        included_files.append(rel_path)

    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel_path in included_files:
            zf.write(project_root / rel_path, arcname=rel_path.as_posix())

    print(
        f"Created Agensi bundle: {archive_path.relative_to(project_root)} ({len(included_files)} files)"
    )
    return archive_path


if __name__ == "__main__":
    build_bundle()

