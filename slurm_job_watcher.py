#!/usr/bin/env python3
"""Watch Slurm jobs and send SMTP email when their state changes."""

import argparse
import configparser
import email.message
import json
import os
import shlex
import smtplib
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.ini"
JOBS_PATH = BASE_DIR / "jobs.json"

ACTIVE_STATES = {
    "PENDING",
    "RUNNING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
    "RESIZING",
    "REQUEUED",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",
}

TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}


class WatcherError(Exception):
    pass


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_cmd(args: List[str], check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)


def load_config() -> configparser.ConfigParser:
    if not CONFIG_PATH.exists():
        raise WatcherError(f"Missing config file: {CONFIG_PATH}. Run: {sys.argv[0]} init")

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def require_config(config: configparser.ConfigParser, section: str, key: str) -> str:
    value = config.get(section, key, fallback="").strip()
    if not value:
        raise WatcherError(f"Missing [{section}] {key} in {CONFIG_PATH}")
    return value


def load_jobs() -> Dict[str, dict]:
    if not JOBS_PATH.exists():
        return {}
    with JOBS_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_jobs(jobs: Dict[str, dict]) -> None:
    tmp = JOBS_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(JOBS_PATH)


def normalize_state(state: str) -> str:
    state = state.strip().upper()
    if not state:
        return "UNKNOWN"
    return state.split()[0]


def parse_pipe_table(output: str) -> Dict[str, str]:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return {}
    parts = lines[0].split("|")
    keys = ["JobID", "JobName", "State", "ExitCode", "Start", "End", "Elapsed", "NodeList"]
    return {key: (parts[i] if i < len(parts) else "") for i, key in enumerate(keys)}


def get_squeue_state(job_id: str) -> Optional[Dict[str, str]]:
    result = run_cmd([
        "squeue",
        "-h",
        "-j",
        job_id,
        "-o",
        "%i|%j|%T|%M|%l|%R",
    ])
    if result.returncode != 0 or not result.stdout.strip():
        return None

    line = result.stdout.strip().splitlines()[0]
    parts = line.split("|")
    return {
        "JobID": parts[0] if len(parts) > 0 else job_id,
        "JobName": parts[1] if len(parts) > 1 else "",
        "State": normalize_state(parts[2] if len(parts) > 2 else ""),
        "Elapsed": parts[3] if len(parts) > 3 else "",
        "TimeLimit": parts[4] if len(parts) > 4 else "",
        "ReasonOrNode": parts[5] if len(parts) > 5 else "",
        "Source": "squeue",
    }


def get_sacct_state(job_id: str) -> Optional[Dict[str, str]]:
    result = run_cmd([
        "sacct",
        "-n",
        "-P",
        "-j",
        job_id,
        "--format=JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList",
    ])
    if result.returncode != 0 or not result.stdout.strip():
        return None

    batch_row = None
    main_row = None
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        row = parse_pipe_table(line)
        row_id = row.get("JobID", "")
        if row_id == job_id:
            main_row = row
        elif row_id == f"{job_id}.batch":
            batch_row = row

    row = main_row or batch_row
    if not row:
        return None

    row["State"] = normalize_state(row.get("State", ""))
    row["Source"] = "sacct"
    return row


def get_job_state(job_id: str) -> Dict[str, str]:
    squeue_info = get_squeue_state(job_id)
    if squeue_info:
        return squeue_info

    sacct_info = get_sacct_state(job_id)
    if sacct_info:
        return sacct_info

    return {"JobID": job_id, "State": "UNKNOWN", "Source": "none"}


def smtp_settings(config: configparser.ConfigParser) -> Tuple[str, int, str, str, str, List[str], bool]:
    host = require_config(config, "smtp", "host")
    port = config.getint("smtp", "port", fallback=465)
    username = require_config(config, "smtp", "username")
    password_env = config.get("smtp", "password_env", fallback="").strip()
    password = config.get("smtp", "password", fallback="").strip()
    if not password and password_env:
        password = os.environ.get(password_env, "")
    if not password:
        if password_env:
            raise WatcherError(f"SMTP password is not set. Put it in [smtp] password or set environment variable {password_env}")
        raise WatcherError("SMTP password is not set. Put it in [smtp] password or configure password_env")

    sender = config.get("smtp", "from", fallback=username).strip() or username
    recipients = [x.strip() for x in require_config(config, "smtp", "to").split(",") if x.strip()]
    use_ssl = config.getboolean("smtp", "use_ssl", fallback=True)
    return host, port, username, password, sender, recipients, use_ssl


def send_email(config: configparser.ConfigParser, subject: str, body: str) -> None:
    host, port, username, password, sender, recipients, use_ssl = smtp_settings(config)

    msg = email.message.EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)

    if use_ssl:
        with smtplib.SMTP_SSL(host, port, timeout=30) as smtp:
            smtp.login(username, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.starttls()
            smtp.login(username, password)
            smtp.send_message(msg)


def format_job_body(job_id: str, job_record: dict, info: Dict[str, str], old_state: str, new_state: str) -> str:
    lines = [
        f"Event time: {now_text()}",
        f"Host: {socket.gethostname()}",
        "",
        f"Job ID: {job_id}",
        f"Name: {job_record.get('name', '')}",
        f"State: {old_state} -> {new_state}",
        f"Source: {info.get('Source', '')}",
        "",
        f"Slurm job name: {info.get('JobName', '')}",
        f"Exit code: {info.get('ExitCode', '')}",
        f"Start: {info.get('Start', '')}",
        f"End: {info.get('End', '')}",
        f"Elapsed: {info.get('Elapsed', '')}",
        f"Node list: {info.get('NodeList', info.get('ReasonOrNode', ''))}",
        f"Time limit: {info.get('TimeLimit', '')}",
        "",
        f"Added at: {job_record.get('added_at', '')}",
        f"Command: {job_record.get('command', '')}",
    ]
    return "\n".join(lines)


def should_notify(old_state: str, new_state: str) -> bool:
    if old_state != new_state:
        return True
    return False


def mark_inactive_if_terminal(job: dict, state: str) -> None:
    if state in TERMINAL_STATES:
        job["active"] = False
        job["finished_at"] = now_text()


def command_init(args: argparse.Namespace) -> None:
    if CONFIG_PATH.exists() and not args.force:
        print(f"Config already exists: {CONFIG_PATH}")
        print("Use --force to overwrite it.")
        return

    config_text = """[smtp]
# Examples:
# QQ mail: smtp.qq.com, port 465, use_ssl true, password is SMTP authorization code.
# 163 mail: smtp.163.com, port 465, use_ssl true, password is SMTP authorization code.
# Gmail: smtp.gmail.com, port 465, use_ssl true, password is app password.
host = smtp.qq.com
port = 465
use_ssl = true
username = your_email@qq.com
from = your_email@qq.com
to = receive_email@example.com
password =
password_env = SLURM_WATCHER_SMTP_PASSWORD

[watch]
interval_seconds = 60
notify_initial_state = true
"""
    CONFIG_PATH.write_text(config_text, encoding="utf-8")
    if not JOBS_PATH.exists():
        save_jobs({})
    print(f"Wrote {CONFIG_PATH}")
    print(f"Wrote {JOBS_PATH}")
    print("Edit config.ini, then set the SMTP password environment variable:")
    print("  export SLURM_WATCHER_SMTP_PASSWORD='your_smtp_authorization_code'")


def command_test_mail(args: argparse.Namespace) -> None:
    config = load_config()
    subject = "[slurm-watcher] test email"
    body = "\n".join([
        "This is a test email from slurm_job_watcher.",
        f"Time: {now_text()}",
        f"Host: {socket.gethostname()}",
    ])
    send_email(config, subject, body)
    print("Test email sent.")


def add_job(job_id: str, name: str = "", command: str = "") -> None:
    jobs = load_jobs()
    info = get_job_state(job_id)
    state = info.get("State", "UNKNOWN")
    jobs[job_id] = {
        "job_id": job_id,
        "name": name or info.get("JobName", ""),
        "last_state": state,
        "active": state not in TERMINAL_STATES,
        "added_at": now_text(),
        "last_checked_at": now_text(),
        "command": command,
    }
    save_jobs(jobs)
    print(f"Added job {job_id}, current state: {state}")


def command_add(args: argparse.Namespace) -> None:
    add_job(args.job_id, args.name or "", "")


def command_submit(args: argparse.Namespace) -> None:
    command = ["sbatch", "--parsable", args.sbatch_file] + args.sbatch_args
    result = run_cmd(command)
    if result.returncode != 0:
        raise WatcherError(f"sbatch failed:\n{result.stderr.strip()}")

    raw_job_id = result.stdout.strip().splitlines()[-1]
    job_id = raw_job_id.split(";")[0]
    add_job(job_id, args.name or "", " ".join(shlex.quote(x) for x in command))
    print(f"Submitted job {job_id}")


def command_list(args: argparse.Namespace) -> None:
    jobs = load_jobs()
    if not jobs:
        print("No watched jobs.")
        return

    for job_id, job in sorted(jobs.items()):
        active = "active" if job.get("active", True) else "done"
        print(f"{job_id}\t{active}\t{job.get('last_state', 'UNKNOWN')}\t{job.get('name', '')}")


def command_remove(args: argparse.Namespace) -> None:
    jobs = load_jobs()
    if args.job_id not in jobs:
        print(f"Job not found: {args.job_id}")
        return
    jobs.pop(args.job_id)
    save_jobs(jobs)
    print(f"Removed job {args.job_id}")


def check_once(config: configparser.ConfigParser, jobs: Dict[str, dict]) -> bool:
    changed = False
    notify_initial = config.getboolean("watch", "notify_initial_state", fallback=True)

    for job_id, job in sorted(jobs.items()):
        if not job.get("active", True):
            continue

        info = get_job_state(job_id)
        old_state = job.get("last_state", "UNKNOWN")
        new_state = info.get("State", "UNKNOWN")
        first_notification_pending = not job.get("initial_notified", False)
        job["last_checked_at"] = now_text()

        notify = should_notify(old_state, new_state)
        if notify_initial and first_notification_pending:
            notify = True

        if notify:
            subject = f"[slurm-watcher] job {job_id} {old_state} -> {new_state}"
            body = format_job_body(job_id, job, info, old_state, new_state)
            send_email(config, subject, body)
            print(f"{now_text()} notified job {job_id}: {old_state} -> {new_state}")
            job["initial_notified"] = True

        if old_state != new_state:
            job["last_state"] = new_state
            changed = True

        old_active = job.get("active", True)
        mark_inactive_if_terminal(job, new_state)
        if old_active != job.get("active", True):
            changed = True

    return changed


def command_run(args: argparse.Namespace) -> None:
    config = load_config()
    interval = args.interval or config.getint("watch", "interval_seconds", fallback=60)

    print(f"Watcher started. interval={interval}s jobs={JOBS_PATH}")
    while True:
        jobs = load_jobs()
        try:
            changed = check_once(config, jobs)
            if changed:
                save_jobs(jobs)
        except Exception as exc:
            print(f"{now_text()} ERROR: {exc}", file=sys.stderr)
        if args.once:
            break
        time.sleep(interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch Slurm jobs and send SMTP email notifications.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create config.ini and jobs.json")
    init_parser.add_argument("--force", action="store_true", help="Overwrite existing config.ini")
    init_parser.set_defaults(func=command_init)

    test_parser = subparsers.add_parser("test-mail", help="Send a test email through SMTP")
    test_parser.set_defaults(func=command_test_mail)

    add_parser = subparsers.add_parser("add", help="Add an existing Slurm job id to watch")
    add_parser.add_argument("job_id")
    add_parser.add_argument("--name", default="")
    add_parser.set_defaults(func=command_add)

    submit_parser = subparsers.add_parser("submit", help="Submit an sbatch file and add the job to watch")
    submit_parser.add_argument("sbatch_file")
    submit_parser.add_argument("--name", default="")
    submit_parser.add_argument("sbatch_args", nargs=argparse.REMAINDER)
    submit_parser.set_defaults(func=command_submit)

    list_parser = subparsers.add_parser("list", help="List watched jobs")
    list_parser.set_defaults(func=command_list)

    remove_parser = subparsers.add_parser("remove", help="Remove a watched job")
    remove_parser.add_argument("job_id")
    remove_parser.set_defaults(func=command_remove)

    run_parser = subparsers.add_parser("run", help="Run the watcher loop")
    run_parser.add_argument("--once", action="store_true", help="Check once and exit")
    run_parser.add_argument("--interval", type=int, default=0, help="Override interval seconds")
    run_parser.set_defaults(func=command_run)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
        return 0
    except WatcherError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
