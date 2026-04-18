from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)
ROOT_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class SlackConfig:
    webhook_url: str
    timeout_seconds: int = 15


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send operation updates or decision notes to Slack webhook."
    )
    parser.add_argument(
        "--message",
        required=True,
        help="Plain text message to post to Slack.",
    )
    parser.add_argument(
        "--title",
        default="Quantify Update",
        help="Header text for Slack block message.",
    )
    parser.add_argument(
        "--status",
        default="INFO",
        choices=("INFO", "SUCCESS", "WARNING", "ERROR"),
        help="Message status badge.",
    )
    parser.add_argument(
        "--context",
        default=None,
        help="Optional short context text (e.g., job id, phase).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload without sending.",
    )
    return parser.parse_args()


def load_config() -> SlackConfig:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or read_env_file_value("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise RuntimeError(
            "SLACK_WEBHOOK_URL is required. Set it in environment variables or .env."
        )
    return SlackConfig(webhook_url=webhook_url)


def read_env_file_value(key: str) -> str | None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return None
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    return None


def build_payload(
    *,
    title: str,
    message: str,
    status: str,
    context: str | None,
) -> dict[str, Any]:
    message = normalize_message(message)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    context_text = f"{context} | {now}" if context else now
    return {
        "text": f"[{status}] {title}\n{message}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"[{status}] {title}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message},
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": context_text}],
            },
        ],
    }


def normalize_message(message: str) -> str:
    # Convert escaped sequences that may come from shell arguments into
    # actual line breaks for better Slack readability.
    return (
        message.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\t", "  ")
        .strip()
    )


def send_webhook(config: SlackConfig, payload: dict[str, Any]) -> None:
    response = requests.post(
        config.webhook_url,
        json=payload,
        timeout=config.timeout_seconds,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Slack webhook failed with status={response.status_code}, body={response.text}"
        )
    LOGGER.info("Slack webhook sent successfully.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    payload = build_payload(
        title=args.title,
        message=args.message,
        status=args.status,
        context=args.context,
    )

    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    config = load_config()
    send_webhook(config, payload)


if __name__ == "__main__":
    main()
