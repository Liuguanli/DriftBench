"""Send the daily DriftBench release summary via Gmail SMTP.

Reads all state from env vars (set by .github/workflows/daily-release.yml):

  GMAIL_USER, GMAIL_APP_PASSWORD, GMAIL_TO  — credentials + destination
  DRIFTBENCH_DATE                            — YYYY-MM-DD
  DRIFTBENCH_VERSION                         — current pyproject.toml version
  DRIFTBENCH_TESTS_OUTCOME                   — success | failure | cancelled | skipped
  DRIFTBENCH_APPROVED                        — true | false
  DRIFTBENCH_GATE_REASON                     — human-readable gate reason
  DRIFTBENCH_RUN_URL                         — link back to the GHA run

Exits 0 if the email is sent or if credentials are missing (skip cleanly).
Exits 1 only on an SMTP error after credentials were present.
"""

from __future__ import annotations

import os
import smtplib
import sys
from email.message import EmailMessage


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def build_message() -> EmailMessage:
    date = env("DRIFTBENCH_DATE", "(unknown date)")
    version = env("DRIFTBENCH_VERSION", "(unknown version)")
    tests = env("DRIFTBENCH_TESTS_OUTCOME", "(unknown)")
    approved = env("DRIFTBENCH_APPROVED", "false") == "true"
    gate_reason = env("DRIFTBENCH_GATE_REASON", "")
    run_url = env("DRIFTBENCH_RUN_URL", "")

    if tests == "success" and approved:
        status = f"SHIPPED v{version}"
    elif tests == "success" and not approved:
        status = f"SKIPPED PyPI upload (tests green, no approval) — staged v{version}"
    elif tests == "failure":
        status = "TESTS FAILED — nothing published"
    else:
        status = f"PARTIAL ({tests})"

    subject = f"[DriftBench daily {date}] {status}"

    body_lines = [
        f"DriftBench daily release run — {date}",
        "",
        f"Status:      {status}",
        f"Version:     {version}",
        f"Tests:       {tests}",
        f"Approved:    {approved}  ({gate_reason})",
        "",
        f"GHA run:     {run_url}",
        "",
        "What this means:",
        "  - SHIPPED:        a new b-version is live on PyPI right now.",
        "  - SKIPPED PyPI:   tests are green but yesterday's approval file is missing.",
        "                    Drop daily/approved/<yesterday>.md to publish on the next run,",
        "                    or wait for tomorrow.",
        "  - TESTS FAILED:   morning routine left the repo on a release branch with red tests.",
        "                    Fix locally, push again, or trigger workflow_dispatch.",
        "",
        "Pipeline: scripts/send_daily_email.py + .github/workflows/daily-release.yml",
    ]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = env("GMAIL_USER")
    msg["To"] = env("GMAIL_TO") or env("GMAIL_USER")
    msg.set_content("\n".join(body_lines))
    return msg


def main() -> int:
    user = env("GMAIL_USER")
    password = env("GMAIL_APP_PASSWORD")

    if not user or not password:
        print("send_daily_email: GMAIL_USER or GMAIL_APP_PASSWORD missing; skipping send.")
        return 0

    msg = build_message()

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(user, password)
            smtp.send_message(msg)
    except smtplib.SMTPException as exc:
        print(f"send_daily_email: SMTP error: {exc}", file=sys.stderr)
        return 1

    print(f"send_daily_email: sent to {msg['To']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
