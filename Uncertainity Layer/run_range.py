#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run a contiguous range of OSeMOSYS scenarios (Excel inputs in Input_Data/)
and stream OSeMOSYS.py logs live to console and file (no model changes).

Features
- Reads Input_Data/LTLE_scenario_XXX.xlsx
- Parallel via --max-procs (be mindful of Gurobi threads)
- Live log streaming: tees child stdout/stderr to console and per-scenario log file
- Mirrors Output_Data/<scenario>/ to Runs/.../artifacts/<scenario>
- Classifies status: success / infeasible / unbounded / failed / missing
- Resume support
"""

import argparse
import csv
import datetime as dt
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional, Tuple

STATUS_FIELDS = [
    "scenario", "status", "return_code", "runtime_s",
    "stdout_log", "out_dir", "started_at", "finished_at"
]

def stem_from_idx(i: int) -> str:
    return f"LTLE_scenario_{i:03d}"

def parse_status_text(text: str) -> str:
    s = text.lower()
    if "solution is:" in s:
        if "infeasible" in s: return "infeasible"
        if "unbounded"  in s: return "unbounded"
        if "optimal"    in s: return "success"
        if "not solved" in s or "undefined" in s: return "failed"
    if "optimisation status" in s and "infeasible" in s:
        return "infeasible"
    if "infeasible" in s: return "infeasible"
    if "unbounded"  in s: return "unbounded"
    return "unknown"

def write_summary_row(summary_path: Path, row: dict):
    new_file = not summary_path.exists()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=STATUS_FIELDS)
        if new_file:
            w.writeheader()
        w.writerow(row)

def scan_summary_status(summary_path: Path, scenario: str) -> Optional[str]:
    if not summary_path.exists():
        return None
    try:
        last_status: Optional[str] = None
        with summary_path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("scenario") == scenario:
                    last_status = row.get("status")
        return last_status
    except Exception:
        return None

def already_done(summary_path: Path, scenario: str) -> bool:
    return scan_summary_status(summary_path, scenario) in (
        "success", "infeasible", "unbounded", "skipped", "missing"
    )

def mirror_outputs(out_src: Path, out_dst: Path):
    if not out_src.exists():
        return None
    out_dst.parent.mkdir(parents=True, exist_ok=True)
    if out_dst.exists():
        return out_dst
    try:
        out_dst.mkdir(parents=True, exist_ok=True)
        for root, dirs, files in os.walk(out_src):
            rel = Path(root).relative_to(out_src)
            (out_dst / rel).mkdir(parents=True, exist_ok=True)
            for fn in files:
                s = Path(root) / fn
                d = (out_dst / rel / fn)
                try:
                    os.link(s, d)  # hardlink
                except Exception:
                    shutil.copy2(s, d)
    except Exception:
        shutil.copytree(out_src, out_dst, dirs_exist_ok=True)
    return out_dst

# ---------- live streaming runner ----------

def run_and_stream(cmd, cwd: Path, log_path: Path, env: dict, echo: bool = True) -> tuple[int, str]:
    """
    Run a command unbuffered and stream its combined stdout/stderr to a file
    (and optionally to console). Returns (return_code, full_text).
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    full_lines = []

    # Open file in text mode for immediate writes
    with log_path.open("w", encoding="utf-8", newline="") as lf:
        # text mode + line buffering
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
        )
        try:
            for line in proc.stdout:
                lf.write(line)
                lf.flush()
                full_lines.append(line)
                if echo:
                    # print without adding extra newline (line already has it)
                    print(line, end="", flush=True)
        finally:
            proc.stdout.close()
            rc = proc.wait()

    return rc, "".join(full_lines)

# ---------------------- worker ----------------------

def worker(task_q: "queue.Queue[Tuple[int, Path]]", *,
           root: Path, input_dir: Path, solver: str, output_format: str,
           python_exec: str, logs_dir: Path, summary_path: Path,
           sleep_between: float, echo_logs: bool):
    while True:
        item = task_q.get()
        if item is None:
            task_q.task_done()
            break

        i, src = item
        stem = stem_from_idx(i)
        xlsx_name = f"{stem}.xlsx"
        out_src = root / "Output_Data" / stem
        artifacts_root = logs_dir.parent / "artifacts"
        out_dst = artifacts_root / stem
        log_path = logs_dir / f"{stem}.log"

        started = dt.datetime.now().isoformat(timespec="seconds")
        status, rc, elapsed, out_dir_str = "failed", 1, None, ""

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"  # make child flush Python logging immediately

            # prepend -u to enforce unbuffered stdio even harder
            cmd = [python_exec, "-u", "OSeMOSYS.py", "-i", xlsx_name, "-s", solver, "-o", output_format]

            t0 = time.time()
            rc, text = run_and_stream(cmd, cwd=root, log_path=log_path, env=env, echo=echo_logs)
            elapsed = round(time.time() - t0, 3)

            cls = parse_status_text(text)
            if rc == 0:
                status = "success" if cls in ("success", "unknown") else cls
            else:
                status = cls if cls != "unknown" else "failed"

            mirrored = mirror_outputs(out_src, out_dst)
            out_dir_str = str(mirrored) if mirrored else ""

        except Exception as e:
            try:
                log_path.write_text(str(e), encoding="utf-8")
            except Exception:
                pass
            status, rc = "error", 1

        finished = dt.datetime.now().isoformat(timespec="seconds")
        write_summary_row(summary_path, {
            "scenario": stem,
            "status": status,
            "return_code": rc,
            "runtime_s": elapsed,
            "stdout_log": str(log_path),
            "out_dir": out_dir_str,
            "started_at": started,
            "finished_at": finished
        })

        if sleep_between > 0:
            time.sleep(sleep_between)

        task_q.task_done()

# ---------------------- CLI ----------------------

def main():
    ap = argparse.ArgumentParser(
        description="Batch runner for OSeMOSYS scenarios from Input_Data/ with live log streaming."
    )
    ap.add_argument("--input-dir", default="Input_Data",
                    help="Folder containing LTLE_scenario_*.xlsx (default: Input_Data)")
    ap.add_argument("--start", type=int, required=True,
                    help="First scenario index (e.g., 1)")
    ap.add_argument("--end", type=int, required=True,
                    help="Last scenario index (e.g., 50)")
    ap.add_argument("--solver", default="gurobi",
                    help="Solver passed to OSeMOSYS.py -s (default: gurobi)")
    ap.add_argument("--output-format", choices=["excel", "csv"], default="csv",
                    help="Use csv for speed (default: csv)")
    ap.add_argument("--python", default=sys.executable,
                    help="Python executable to run OSeMOSYS.py")
    ap.add_argument("--max-procs", type=int, default=1,
                    help="Number of scenarios to run in parallel")
    ap.add_argument("--resume", action="store_true",
                    help="Skip scenarios already recorded with terminal status in summary")
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="Seconds to sleep between job starts")
    ap.add_argument("--summary", default=None,
                    help="Optional path for run_summary.csv")
    ap.add_argument("--quiet", action="store_true",
                    help="Do not echo child logs to console; still write to per-scenario .log")
    args = ap.parse_args()

    root = Path.cwd()
    input_dir = (root / args.input_dir).resolve()

    stamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_root = root / "Runs" / f"{args.start:03d}-{args.end:03d}_{stamp}"
    logs_dir = run_root / "logs"
    artifacts_dir = run_root / "artifacts"
    run_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    summary_path = Path(args.summary) if args.summary else (run_root / "run_summary.csv")

    # queue scenarios
    q: "queue.Queue[Tuple[int, Path]]" = queue.Queue()
    for i in range(args.start, args.end + 1):
        stem = stem_from_idx(i)
        src = input_dir / f"{stem}.xlsx"
        if not src.exists():
            write_summary_row(summary_path, {
                "scenario": stem, "status": "missing", "return_code": None,
                "runtime_s": None, "stdout_log": None, "out_dir": "",
                "started_at": None, "finished_at": None
            })
            continue
        if args.resume and already_done(summary_path, stem):
            write_summary_row(summary_path, {
                "scenario": stem, "status": "skipped", "return_code": 0,
                "runtime_s": 0.0, "stdout_log": None, "out_dir": "",
                "started_at": None, "finished_at": None
            })
            continue
        q.put((i, src))

    # start workers
    procs = max(1, args.max_procs)
    threads = []
    for _ in range(procs):
        t = threading.Thread(target=worker, kwargs=dict(
            task_q=q, root=root, input_dir=input_dir, solver=args.solver,
            output_format=args.output_format, python_exec=args.python,
            logs_dir=logs_dir, summary_path=summary_path,
            sleep_between=args.sleep, echo_logs=not args.quiet
        ), daemon=True)
        t.start()
        threads.append(t)

    # stop sentinels & wait
    for _ in threads:
        q.put(None)
    q.join()

    print(f"\nDone.\nSummary → {summary_path}\nLogs → {logs_dir}\nArtifacts → {artifacts_dir}")

if __name__ == "__main__":
    main()
