"""Reproducible synthetic log generator (fixed seed) with labelled defects and ground truth.

For each dataset it writes
    * a JSON-Lines file  (structured logs)
    * a text file        (semi-structured logs, regex-parsed)
    * an expected_results*.json file computed from the generator's OWN labels, i.e. independently of the
      pipeline - this is what the pipeline output is verified against.

Run:  python -m src.generator            (all datasets)
      python -m src.generator --dataset sample
"""
from __future__ import annotations

import argparse
import json
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import config

GENERATOR_VERSION = "1.0"

DATASETS = {
    "sample": dict(n=100, start="2026-10-01T00:00:00", dir=config.SAMPLE_DIR, json="structured_logs.jsonl",
                   text="application_logs.txt", expected="expected_results.json"),
    "10k": dict(n=10_000, start="2026-10-02T00:00:00", dir=config.GENERATED_DIR, json="logs_10k.jsonl",
                text="logs_10k.txt", expected="expected_results_10k.json"),
    "100k": dict(n=100_000, start="2026-10-05T00:00:00", dir=config.GENERATED_DIR, json="logs_100k.jsonl",
                 text="logs_100k.txt", expected="expected_results_100k.json"),
}

SERVICES = list(config.KNOWN_SERVICES)
SERVICE_WEIGHTS = {"auth": 22, "gateway": 20, "orders": 16, "payment": 14, "inventory": 10,
                   "notifications": 10, "search": 8}
BASE_SEV = {"INFO": 72, "WARNING": 17, "ERROR": 8, "CRITICAL": 3}
SERVICE_SEV = {"payment": {"INFO": 58, "WARNING": 20, "ERROR": 16, "CRITICAL": 6},
               "gateway": {"INFO": 66, "WARNING": 20, "ERROR": 10, "CRITICAL": 4}}
LATENCY = {"INFO": (120, 45), "WARNING": (420, 150), "ERROR": (1800, 700), "CRITICAL": (4200, 1500)}
LATENCY_MULT = {"payment": 1.4, "search": 1.2, "auth": 0.9, "gateway": 0.8}

MESSAGES = {
    "INFO": {"auth": ["User login successful", "User logout successful", "Token refreshed"],
             "payment": ["Payment authorized", "Refund processed", "Invoice generated"],
             "inventory": ["Stock level synced", "Item reserved", "Warehouse sync completed"],
             "orders": ["Order created", "Order shipped", "Order status updated"],
             "notifications": ["Email notification sent", "Push notification delivered"],
             "search": ["Search query completed", "Index refreshed"],
             "gateway": ["Request routed", "Health check passed"]},
    "WARNING": {"auth": ["Multiple failed login attempts", "Session nearing expiry"],
                "payment": ["Slow response from card processor", "Retry attempt scheduled"],
                "inventory": ["Low stock detected", "Stock mismatch detected"],
                "orders": ["Order processing delayed", "High memory usage"],
                "notifications": ["Notification queue backlog", "Delivery retry scheduled"],
                "search": ["Cache miss ratio high", "Slow query detected"],
                "gateway": ["Rate limit nearing threshold", "High memory usage"]},
    "ERROR": {"auth": ["Invalid token signature", "Authentication backend timeout"],
              "payment": ["Database connection failed", "Transaction processing failed"],
              "inventory": ["Inventory update failed", "Database connection failed"],
              "orders": ["Order persistence failed", "Upstream timeout"],
              "notifications": ["Failed to send notification", "SMTP connection refused"],
              "search": ["Index query failed", "Shard unavailable"],
              "gateway": ["Upstream timeout", "Bad gateway response"]},
    "CRITICAL": {"auth": ["Authentication service down", "Credential store unreachable"],
                 "payment": ["Payment service unavailable", "Ledger write failure"],
                 "inventory": ["Inventory database unreachable", "Disk space exhausted"],
                 "orders": ["Order service crashed", "Message queue unavailable"],
                 "notifications": ["Notification service down", "Out of memory - service restarting"],
                 "search": ["Search cluster unreachable", "Index corruption detected"],
                 "gateway": ["Gateway unavailable", "TLS certificate expired"]},
}

SEV_VARIANTS = {"INFO": ["info", "Info"], "WARNING": ["warn", "WARN", "Warning"],
                "ERROR": ["err", "error", "Err"], "CRITICAL": ["crit", "critical", "Critical"]}
BAD_SEVERITIES = ["DEBUG", "TRACE", "VERBOSE", "NOTICE", "UNKNOWN", "INVALID"]
BAD_TS_JSON = ["05/11/2026 10:17:30", "2026-13-45T10:00:00Z", "yesterday", "2026-11-05 25:61:00",
               "11-05-2026", "not-a-timestamp", "2026/11/05 10:00:00"]
BAD_TS_TEXT = ["05/11/2026 10:17:30", "2026-13-45 10:00:00", "2026-11-05 25:61:00", "11-05-2026 10:00:00",
               "yesterday 10:00:00"]
BAD_LATENCY = ["abc", "slow", "12ms3", "--", "fast"]
BAD_SERVICES = ["legacy-billing", "mailer", "analytics-v0", "reporting"]
BAD_EVENT_IDS = ["bad id!", "id/with/slash", "#$%", "x" * 80]
TEXT_GARBAGE = ["### corrupted line ###", "ERROR payment Database connection failed", "{{{{",
                "2026-10-01 ERROR missing time and brackets", "??? partial write"]
UNEXPECTED_JSON = ['{"foo":"bar","value":42}', "[1,2,3]", '"just a string"',
                   '{"metric":"cpu","host":"srv1"}', "42"]
OFFSETS = ["+05:30", "+01:00", "-08:00", "+09:00", "-05:00"]

# quota = max(1, round(share * base_records)) -> every defect type appears even in the 100-record sample
DEFECT_SHARES = {
    "missing_event_id": .005, "missing_service": .005, "missing_message": .005, "missing_severity": .005,
    "missing_timestamp": .005, "invalid_timestamp": .015, "invalid_severity": .010, "negative_latency": .010,
    "invalid_latency_type": .005, "latency_exceeds_max": .005, "unknown_service": .010,
    "invalid_event_id": .005, "malformed": .015, "unexpected_schema": .005,
}
JSON_ONLY = {"missing_event_id", "missing_severity", "missing_timestamp", "invalid_event_id", "unexpected_schema"}
DUP_SHARE, LATE_SHARE, SWAP_SHARE, NO_LATENCY_SHARE, TEXT_SHARE = .02, .02, .03, .03, .30

KIND_REASON = {
    "missing_event_id": "MISSING_EVENT_ID", "missing_service": "MISSING_SERVICE_NAME",
    "missing_message": "MISSING_MESSAGE", "missing_severity": "MISSING_SEVERITY",
    "missing_timestamp": "MISSING_TIMESTAMP", "invalid_timestamp": "INVALID_TIMESTAMP",
    "invalid_severity": "INVALID_SEVERITY", "negative_latency": "NEGATIVE_LATENCY",
    "invalid_latency_type": "INVALID_LATENCY_TYPE", "latency_exceeds_max": "LATENCY_EXCEEDS_MAX",
    "unknown_service": "UNKNOWN_SERVICE", "invalid_event_id": "INVALID_EVENT_ID",
    "unexpected_schema": "UNEXPECTED_SCHEMA",
}


@dataclass
class Rec:
    idx: int
    event_id: str
    ts: datetime
    sev: str
    svc: str
    msg: str
    lat: Optional[int]
    fmt: str = "json"
    kind: str = "valid"
    line: str = ""


def percentile(values: List[float], q: float) -> Optional[float]:
    """Linear-interpolation percentile (independent re-implementation, matches numpy/pandas default)."""
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * q / 100.0
    lo, hi = math.floor(k), min(math.floor(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def _r(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 4)


def _lat_stats(vals: List[int]) -> dict:
    if not vals:
        return {"count": 0, "avg_ms": None, "min_ms": None, "max_ms": None, "p50_ms": None, "p95_ms": None}
    return {"count": len(vals), "avg_ms": _r(sum(vals) / len(vals)), "min_ms": _r(min(vals)),
            "max_ms": _r(max(vals)), "p50_ms": _r(percentile(vals, 50)), "p95_ms": _r(percentile(vals, 95))}


def _base_record(rng: random.Random, i: int, tag: str, ts: datetime) -> Rec:
    svc = rng.choices(SERVICES, weights=[SERVICE_WEIGHTS[s] for s in SERVICES])[0]
    w = SERVICE_SEV.get(svc, BASE_SEV)
    sev = rng.choices(list(w), weights=list(w.values()))[0]
    mu, sd = LATENCY[sev]
    mult = LATENCY_MULT.get(svc, 1.0)
    lat = None if rng.random() < NO_LATENCY_SHARE else max(1, int(rng.gauss(mu * mult, sd * mult)))
    msg = rng.choice(MESSAGES[sev][svc])
    fmt = "text" if rng.random() < TEXT_SHARE else "json"
    return Rec(i, f"EVT-{tag}-{i + 1:06d}", ts, sev, svc, msg, lat, fmt)


def _render(rec: Rec, rng: random.Random) -> str:
    """Render one record in its format, applying legal variants (valid) or its defect (invalid)."""
    kind, fmt = rec.kind, rec.fmt
    sev, svc, msg, lat, eid = rec.sev, rec.svc, rec.msg, rec.lat, rec.event_id
    ts_iso = rec.ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    ts_txt = rec.ts.strftime("%Y-%m-%d %H:%M:%S")

    # ---- harmless variants (still valid) ------------------------------------------------
    if rng.random() < .10:
        sev = rng.choice(SEV_VARIANTS[sev])
    if rng.random() < .03:
        svc = rng.choice([svc.upper(), f" {svc} "])
    if rng.random() < .03:
        msg = msg + "  "
    ts_json = ts_iso
    r = rng.random()
    if r >= .80:
        if r < .88:
            ts_json = ts_txt
        elif r < .95:
            off = rng.choice(OFFSETS)
            sign = 1 if off[0] == "+" else -1
            local = rec.ts + sign * timedelta(hours=int(off[1:3]), minutes=int(off[4:6]))
            ts_json = local.strftime("%Y-%m-%dT%H:%M:%S") + off
        else:
            ts_json = rec.ts.strftime("%Y-%m-%dT%H:%M:%S") + f".{rng.randint(0, 999):03d}Z"
    lat_val: object = lat
    if fmt == "json" and lat is not None and rng.random() < .03:
        lat_val = str(lat)
    alt_keys = fmt == "json" and rng.random() < .03

    ts_out = ts_json if fmt == "json" else ts_txt
    sev_out, svc_out, msg_out, eid_out = sev, svc, msg, eid

    # ---- defects ------------------------------------------------------------------------
    miss = lambda: rng.choice(["DROP", None, "", "   "])
    if kind == "missing_event_id":
        eid_out = miss()
    elif kind == "missing_service":
        svc_out = miss() if fmt == "json" else rng.choice(["", " "])
    elif kind == "missing_message":
        msg_out = miss() if fmt == "json" else ""
    elif kind == "missing_severity":
        sev_out = miss()
    elif kind == "missing_timestamp":
        ts_out = miss()
    elif kind == "invalid_timestamp":
        ts_out = rng.choice(BAD_TS_JSON if fmt == "json" else BAD_TS_TEXT)
    elif kind == "invalid_severity":
        sev_out = rng.choice(BAD_SEVERITIES)
    elif kind == "negative_latency":
        lat_val = -rng.randint(1, 5000)
    elif kind == "invalid_latency_type":
        lat_val = rng.choice(BAD_LATENCY)
    elif kind == "latency_exceeds_max":
        lat_val = rng.randint(10_000_000, 99_999_999)
    elif kind == "unknown_service":
        svc_out = rng.choice(BAD_SERVICES)
    elif kind == "invalid_event_id":
        eid_out = rng.choice(BAD_EVENT_IDS)
    elif kind == "unexpected_schema":
        return rng.choice(UNEXPECTED_JSON)
    elif kind == "malformed" and fmt == "text":
        return rng.choice(TEXT_GARBAGE)

    if fmt == "json":
        sev_key, svc_key = ("level", "service") if alt_keys else ("severity", "service_name")
        items = [("event_id", eid_out), ("timestamp", ts_out), (sev_key, sev_out), (svc_key, svc_out),
                 ("message", msg_out), ("latency_ms", lat_val)]
        doc = {k: v for k, v in items if v != "DROP" and not (k == "latency_ms" and v is None)}
        line = json.dumps(doc, separators=(",", ":"))
        if kind == "malformed":
            line = line[:rng.randint(10, max(11, len(line) - 3))]
        return line

    line = f"{ts_out} {sev_out} [{svc_out}] {msg_out}".rstrip() if msg_out else f"{ts_out} {sev_out} [{svc_out}]"
    if lat_val is not None:
        line += f" latency_ms={lat_val}"
    return line


def generate_dataset(name: str) -> Tuple[List[str], List[str], dict]:
    spec = DATASETS[name]
    n = spec["n"]
    rng = random.Random(f"{config.SEED}:{name}")      # fixed seed, independent stream per dataset
    tag = name.upper()
    n_dup = max(1, round(n * DUP_SHARE))
    n_base = n - n_dup

    # 1. base records with strictly increasing timestamps
    ts = datetime.fromisoformat(spec["start"]).replace(tzinfo=timezone.utc)
    base: List[Rec] = []
    for i in range(n_base):
        ts += timedelta(seconds=rng.randint(1, 20))
        base.append(_base_record(rng, i, tag, ts))

    # 2. assign defects to distinct positions
    quotas = {k: max(1, round(n_base * s)) for k, s in DEFECT_SHARES.items()}
    positions = rng.sample(range(n_base), sum(quotas.values()))
    cursor = 0
    for kind, q in quotas.items():
        for pos in positions[cursor:cursor + q]:
            base[pos].kind = kind
            if kind in JSON_ONLY:
                base[pos].fmt = "json"
        cursor += q

    # 3. late events: valid records get an older timestamp but keep their position in the stream
    valid_idx = [r.idx for r in base if r.kind == "valid" and r.idx >= 10]
    for idx in rng.sample(valid_idx, max(1, round(n_base * LATE_SHARE))):
        base[idx].ts -= timedelta(seconds=rng.randint(3600, 6 * 3600))

    # 4. text-format valid records need unique content (their event_id is a content hash)
    seen = set()
    for r in base:
        if r.fmt == "text" and r.kind == "valid":
            while (key := (r.ts, r.sev, r.svc, r.msg, r.lat)) in seen:
                r.lat = (r.lat or 0) + 1
            seen.add(key)

    # 5. out-of-order: swap nearby records (timestamps unchanged)
    for _ in range(max(1, round(n_base * SWAP_SHARE))):
        i = rng.randrange(n_base - 1)
        j = min(n_base - 1, i + rng.randint(1, 5))
        base[i], base[j] = base[j], base[i]

    # 6. render, then insert exact duplicates (repeated event id) after their originals
    for r in base:
        r.line = _render(r, rng)
    valid = [r for r in base if r.kind == "valid"]
    position = {id(r): p for p, r in enumerate(base)}
    entries = [(float(p), r, False) for p, r in enumerate(base)]
    for src in rng.sample(valid, n_dup):          # a duplicate always arrives AFTER its original
        entries.append((position[id(src)] + rng.uniform(0.5, 50.0), src, True))
    entries.sort(key=lambda e: e[0])

    json_lines = [e[1].line for e in entries if e[1].fmt == "json"]
    text_lines = [e[1].line for e in entries if e[1].fmt == "text"]
    expected = _expected(name, n, entries, valid)
    return json_lines, text_lines, expected


def _expected(name: str, n: int, entries, valid: List[Rec]) -> dict:
    reasons: Counter = Counter()
    for _, r, is_dup in entries:
        if is_dup or r.kind == "valid":
            continue
        reasons["PARSE_ERROR_MALFORMED_JSON" if (r.kind == "malformed" and r.fmt == "json")
                else "PARSE_ERROR_UNEXPECTED_FORMAT" if r.kind == "malformed"
                else KIND_REASON[r.kind]] += 1

    # late / out-of-order over first occurrences, per file, in file order (fresh-database assumption)
    late = ooo = 0
    thr = config.LATE_THRESHOLD_MINUTES * 60
    for fmt in ("json", "text"):
        running = None
        for _, r, is_dup in entries:
            if r.fmt != fmt or is_dup or r.kind != "valid":
                continue
            e = r.ts.timestamp()
            if running is not None:
                ooo += e < running
                late += (running - e) > thr
            running = e if running is None else max(running, e)

    errors = [r for r in valid if r.sev in config.ERROR_SEVERITIES]
    sev_counts = {s: sum(1 for r in valid if r.sev == s) for s in config.VALID_SEVERITIES}
    by_service, lat_by_service = {}, {}
    for svc in SERVICES:
        rs = [r for r in valid if r.svc == svc]
        e = sum(1 for r in rs if r.sev in config.ERROR_SEVERITIES)
        if rs:
            by_service[svc] = {"total": len(rs), "errors": e, "error_rate_pct": _r(e / len(rs) * 100)}
            lat_by_service[svc] = _lat_stats([r.lat for r in rs if r.lat is not None])
    daily = defaultdict(lambda: {"total": 0, "errors": 0})
    for r in valid:
        d = daily[r.ts.strftime("%Y-%m-%d")]
        d["total"] += 1
        d["errors"] += r.sev in config.ERROR_SEVERITIES
    n_dup = sum(1 for e in entries if e[2])
    lat = _lat_stats([r.lat for r in valid if r.lat is not None])
    lat["by_service"] = lat_by_service
    return {
        "dataset": name, "generator_version": GENERATOR_VERSION, "seed": config.SEED,
        "note": "Ground truth computed by the generator from its own labels, independent of the pipeline.",
        "input_records": n,
        "json_records": sum(1 for e in entries if e[1].fmt == "json"),
        "text_records": sum(1 for e in entries if e[1].fmt == "text"),
        "outcomes": {"inserted": len(valid), "duplicates": n_dup, "rejected": sum(reasons.values())},
        "rejected_by_reason": dict(sorted(reasons.items())),
        "late_records": int(late), "out_of_order_records": int(ooo),
        "database_state": {
            "total_logs": len(valid), "severity_counts": sev_counts, "error_count": len(errors),
            "error_rate_pct": _r(len(errors) / len(valid) * 100),
            "error_rate_by_service": by_service, "latency": lat,
            "daily": dict(sorted(daily.items())),
        },
    }


def write_dataset(name: str, out_dir: Optional[Path] = None) -> Dict[str, Path]:
    spec = DATASETS[name]
    out = Path(out_dir) if out_dir else Path(spec["dir"])
    out.mkdir(parents=True, exist_ok=True)
    json_lines, text_lines, expected = generate_dataset(name)
    paths = {"json": out / spec["json"], "text": out / spec["text"], "expected": out / spec["expected"]}
    for key, lines in (("json", json_lines), ("text", text_lines)):
        with open(paths[key], "w", encoding="utf-8", newline="\n") as fh:
            fh.write("\n".join(lines) + "\n")
    paths["expected"].write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
    return paths


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Generate reproducible synthetic LogPulse datasets")
    ap.add_argument("--dataset", choices=[*DATASETS, "all"], default="all")
    ap.add_argument("--out-dir", default=None, help="override output directory")
    args = ap.parse_args(argv)
    for name in (DATASETS if args.dataset == "all" else [args.dataset]):
        paths = write_dataset(name, args.out_dir)
        print(f"[{name}] " + ", ".join(f"{k}={v}" for k, v in paths.items()))


if __name__ == "__main__":
    main()
