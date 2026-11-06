"""Ingestion layer: discover files, read them safely, capture provenance."""
from __future__ import annotations

import glob
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

from . import config
from .logger import get_logger

log = get_logger("ingestion")

JSON_EXTS = {".jsonl", ".ndjson"}
TEXT_EXTS = {".txt", ".log"}


@dataclass
class RawRecord:
    source_file: str
    file_sha256: str
    line_number: int
    raw: str
    source_format: str  # 'json' | 'text'


@dataclass
class IngestedFile:
    path: str
    name: str
    status: str  # OK | EMPTY | MISSING | UNREADABLE
    sha256: Optional[str] = None
    size_bytes: int = 0
    error: Optional[str] = None
    blank_lines: int = 0
    records: List[RawRecord] = field(default_factory=list)

    @property
    def readable(self) -> bool:
        return self.status in ("OK", "EMPTY")


def discover_files(inputs: Iterable) -> List[Path]:
    """Expand files / directories / globs into an ordered, de-duplicated file list.

    Directories are scanned non-recursively for supported extensions; helper files such as
    ``expected_results*.json`` are naturally skipped because ``.json`` is not an input format.
    Missing explicit paths are kept so ingestion can report them as failures.
    """
    found: List[Path] = []
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            found.extend(sorted(f for f in p.iterdir()
                                if f.is_file() and f.suffix.lower() in config.SUPPORTED_EXTENSIONS))
        elif not p.exists() and any(c in str(item) for c in "*?["):
            found.extend(Path(m) for m in sorted(glob.glob(str(item))))
        else:
            found.append(p)
    seen, ordered = set(), []
    for f in found:
        key = str(f.resolve()) if f.exists() else str(f)
        if key not in seen:
            seen.add(key)
            ordered.append(f)
    return ordered


def _file_format(path: Path) -> Optional[str]:
    ext = path.suffix.lower()
    if ext in JSON_EXTS:
        return "json"
    if ext in TEXT_EXTS:
        return "text"
    return None


def _sniff(line: str) -> str:
    return "json" if line.lstrip().startswith(("{", "[")) else "text"


def read_log_file(path) -> IngestedFile:
    """Read one file. Never raises: problems are reported through ``status``/``error``."""
    p = Path(path)
    result = IngestedFile(path=str(p), name=p.name, status="OK")
    if not p.exists():
        result.status, result.error = "MISSING", "File not found"
        return result
    if p.is_dir():
        result.status, result.error = "UNREADABLE", "Path is a directory"
        return result
    try:
        data = p.read_bytes()
    except OSError as exc:
        result.status, result.error = "UNREADABLE", f"{type(exc).__name__}: {exc}"
        return result
    if b"\x00" in data[:8192]:
        result.status, result.error = "UNREADABLE", "Binary file (NUL bytes detected)"
        return result

    result.sha256 = hashlib.sha256(data).hexdigest()
    result.size_bytes = len(data)
    text = data.decode("utf-8-sig", errors="replace")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # trailing newline is a terminator, not a blank line

    file_fmt = _file_format(p)
    for number, line in enumerate(lines, start=1):
        line = line.rstrip("\r")
        if not line.strip():
            result.blank_lines += 1
            continue
        result.records.append(RawRecord(p.name, result.sha256, number, line, file_fmt or _sniff(line)))
    if not result.records:
        result.status = "EMPTY"
        log.warning("File %s is empty (no records)", p.name)
    return result
