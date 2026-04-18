from __future__ import annotations

import argparse
import logging
import os
import traceback
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
UNIVERSE_DIR = DATA_DIR / "universe"

OPENDART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
OPENDART_FINANCIALS_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"

REPORT_CODES = {
    "annual": "11011",
    "half": "11012",
    "q1": "11013",
    "q3": "11014",
}


@dataclass(frozen=True)
class DartConfig:
    api_key: str
    start_year: int
    end_year: int
    report_code: str
    throttle_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OpenDART corporation codes and major financial accounts."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("OPEN_DART_API_KEY") or read_env_file_value("OPEN_DART_API_KEY"),
        help="OpenDART API key. Defaults to OPEN_DART_API_KEY env var.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Start business year to request. Default is 2020.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=pd.Timestamp.today().year,
        help="End business year to request. Default is current year.",
    )
    parser.add_argument(
        "--report",
        choices=sorted(REPORT_CODES),
        default="annual",
        help="OpenDART report type shortcut. Default is annual.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        help="Optional list of stock tickers. If omitted, use the latest universe snapshot.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit after filtering tickers.",
    )
    parser.add_argument(
        "--throttle-seconds",
        type=float,
        default=0.0,
        help="Optional sleep interval between API requests.",
    )
    return parser.parse_args()


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


def get_slack_webhook_url() -> str | None:
    return os.getenv("SLACK_WEBHOOK_URL") or read_env_file_value("SLACK_WEBHOOK_URL")


def send_slack_event(title: str, message: str, status: str, context: str | None = None) -> None:
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        return

    payload: dict[str, object] = {
        "text": f"[{status}] {title}: {message}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"[{status}] {title}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message},
            },
        ],
    }
    if context:
        payload["blocks"].append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": context}],
            }
        )

    try:
        response = requests.post(webhook_url, json=payload, timeout=15)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        LOGGER.warning("Failed to send Slack event: %s", exc)


def ensure_directories() -> None:
    for path in (RAW_DIR / "opendart", UNIVERSE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def build_config(args: argparse.Namespace) -> DartConfig:
    if not args.api_key:
        raise RuntimeError(
            "OpenDART API key is required. Pass --api-key or set OPEN_DART_API_KEY."
        )
    if args.start_year > args.end_year:
        raise ValueError("start-year must be less than or equal to end-year.")
    return DartConfig(
        api_key=args.api_key,
        start_year=args.start_year,
        end_year=args.end_year,
        report_code=REPORT_CODES[args.report],
        throttle_seconds=args.throttle_seconds,
    )


def fetch_corp_codes(api_key: str) -> pd.DataFrame:
    response = requests.get(
        OPENDART_CORP_CODE_URL,
        params={"crtfc_key": api_key},
        timeout=60,
    )
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        xml_name = archive.namelist()[0]
        xml_bytes = archive.read(xml_name)

    root = ET.fromstring(xml_bytes)
    rows: list[dict[str, str]] = []
    for item in root.findall("list"):
        rows.append(
            {
                "corp_code": (item.findtext("corp_code") or "").strip(),
                "corp_name": (item.findtext("corp_name") or "").strip(),
                "stock_code": (item.findtext("stock_code") or "").strip(),
                "modify_date": (item.findtext("modify_date") or "").strip(),
            }
        )

    frame = pd.DataFrame(rows)
    frame = frame[frame["stock_code"] != ""].copy()
    frame["collected_at"] = pd.Timestamp.utcnow()
    return frame.sort_values("stock_code").reset_index(drop=True)


def save_corp_codes(frame: pd.DataFrame) -> Path:
    output_path = UNIVERSE_DIR / "dart_corp_codes.csv"
    frame.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def latest_universe_snapshot() -> Path:
    candidates = sorted(UNIVERSE_DIR.glob("universe_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            "No universe snapshot found. Run quantify.data_loader first."
        )
    return candidates[-1]


def select_tickers(
    requested: Iterable[str] | None,
    limit: int | None,
) -> list[str]:
    if requested:
        tickers = list(dict.fromkeys(requested))
    else:
        universe_path = latest_universe_snapshot()
        universe = pd.read_csv(universe_path, dtype={"ticker": str})
        tickers = universe["ticker"].astype(str).tolist()

    if limit is not None:
        tickers = tickers[:limit]
    return tickers


def fetch_financial_statement(
    api_key: str,
    corp_code: str,
    business_year: int,
    report_code: str,
) -> pd.DataFrame:
    response = requests.get(
        OPENDART_FINANCIALS_URL,
        params={
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": str(business_year),
            "reprt_code": report_code,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    status = payload.get("status")
    message = payload.get("message", "")
    if status != "000":
        if status in {"013", "020"}:
            LOGGER.info(
                "Skipping corp_code=%s year=%s: %s (%s)",
                corp_code,
                business_year,
                message,
                status,
            )
            return pd.DataFrame()
        raise RuntimeError(
            f"OpenDART request failed for corp_code={corp_code}, year={business_year}: "
            f"{message} ({status})"
        )

    frame = pd.DataFrame(payload.get("list", []))
    if frame.empty:
        return frame

    frame["bsns_year"] = business_year
    frame["report_code"] = report_code
    return frame


def save_financials(frame: pd.DataFrame, ticker: str) -> Path | None:
    if frame.empty:
        return None
    output_path = RAW_DIR / "opendart" / f"{ticker}_financials.csv"
    frame.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def run_collection(config: DartConfig, tickers: Iterable[str], limit: int | None) -> dict[str, int]:
    ensure_directories()

    LOGGER.info("Fetching OpenDART corp code mapping")
    corp_codes = fetch_corp_codes(config.api_key)
    corp_code_path = save_corp_codes(corp_codes)
    LOGGER.info("Saved corp code mapping: %s", corp_code_path)

    target_tickers = select_tickers(tickers, limit)
    corp_map = corp_codes.set_index("stock_code")
    missing = [ticker for ticker in target_tickers if ticker not in corp_map.index]
    if missing:
        LOGGER.warning("Tickers missing from OpenDART corp code mapping: %s", ", ".join(missing))

    selected = [ticker for ticker in target_tickers if ticker in corp_map.index]
    LOGGER.info("Collecting OpenDART financials for %s tickers", len(selected))
    saved_count = 0
    empty_count = 0

    for index, ticker in enumerate(selected, start=1):
        corp_row = corp_map.loc[ticker]
        corp_code = str(corp_row["corp_code"])
        corp_name = str(corp_row["corp_name"])
        LOGGER.info("[%s/%s] Fetching %s %s", index, len(selected), ticker, corp_name)

        frames: list[pd.DataFrame] = []
        for business_year in range(config.start_year, config.end_year + 1):
            statement = fetch_financial_statement(
                api_key=config.api_key,
                corp_code=corp_code,
                business_year=business_year,
                report_code=config.report_code,
            )
            if statement.empty:
                continue
            statement["ticker"] = ticker
            statement["corp_name"] = corp_name
            frames.append(statement)

            if config.throttle_seconds > 0:
                import time

                time.sleep(config.throttle_seconds)

        if not frames:
            LOGGER.warning("No OpenDART financials returned for %s", ticker)
            empty_count += 1
            continue

        combined = pd.concat(frames, ignore_index=True)
        output_path = save_financials(combined, ticker)
        LOGGER.info("Saved %s rows to %s", len(combined), output_path)
        saved_count += 1

    return {
        "target_tickers": len(target_tickers),
        "selected_tickers": len(selected),
        "saved_tickers": saved_count,
        "empty_tickers": empty_count,
        "missing_tickers": len(missing),
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = parse_args()
    config = build_config(args)
    context = f"report={args.report}, years={config.start_year}-{config.end_year}"
    send_slack_event(
        title="OpenDART Collection Started",
        message="OpenDART financial collection has started.",
        status="INFO",
        context=context,
    )

    try:
        stats = run_collection(config, args.tickers, args.limit)
    except Exception as exc:
        send_slack_event(
            title="OpenDART Collection Failed",
            message=f"Collection failed: {exc}",
            status="ERROR",
            context=traceback.format_exc(limit=1),
        )
        raise

    summary = (
        f"saved={stats['saved_tickers']}, empty={stats['empty_tickers']}, "
        f"missing={stats['missing_tickers']}, selected={stats['selected_tickers']}"
    )
    send_slack_event(
        title="OpenDART Collection Completed",
        message=f"OpenDART collection finished. {summary}",
        status="SUCCESS",
        context=context,
    )


if __name__ == "__main__":
    main()
