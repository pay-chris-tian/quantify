from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DART_DIR = ROOT_DIR / "data" / "raw" / "opendart"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

ACCOUNT_MAP = {
    "assets_total": ["자산총계"],
    "liabilities_total": ["부채총계"],
    "equity_total": ["자본총계"],
    "revenue": ["매출액", "영업수익"],
    "operating_income": ["영업이익"],
    "net_income": ["당기순이익", "당기순이익(손실)", "반기순이익", "분기순이익"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert OpenDART raw statements into annual factor-ready fundamentals."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional single year filter.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional ticker filter.",
    )
    return parser.parse_args()


def ensure_output_dir() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def parse_amount(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "N/A", "nan"}:
        return None
    clean = re.sub(r"[,\s]", "", text)
    try:
        return float(clean)
    except ValueError:
        return None


def find_mapped_value(group: pd.DataFrame, candidates: list[str]) -> float | None:
    for account_name in candidates:
        rows = group[group["account_nm"] == account_name]
        if rows.empty:
            continue
        value = parse_amount(rows.iloc[0].get("thstrm_amount"))
        if value is not None:
            return value
    return None


def to_annual_fundamentals(frame: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"ticker", "corp_name", "bsns_year", "account_nm", "thstrm_amount"}
    missing = required_cols - set(frame.columns)
    if missing:
        raise ValueError(f"Missing columns in OpenDART raw data: {', '.join(sorted(missing))}")

    grouped_rows: list[dict[str, object]] = []
    grouped = frame.groupby(["ticker", "corp_name", "bsns_year"], dropna=False)
    for (ticker, corp_name, year), group in grouped:
        record: dict[str, object] = {
            "ticker": str(ticker),
            "corp_name": str(corp_name),
            "year": int(year),
        }
        for target, account_names in ACCOUNT_MAP.items():
            record[target] = find_mapped_value(group, account_names)
        grouped_rows.append(record)

    output = pd.DataFrame(grouped_rows).sort_values(["ticker", "year"]).reset_index(drop=True)
    output["debt_ratio"] = (
        output["liabilities_total"] / output["equity_total"]
    ) * 100.0
    output["roe"] = (output["net_income"] / output["equity_total"]) * 100.0
    output["collected_at"] = pd.Timestamp.utcnow()
    return output


def load_raw_frames(tickers: list[str] | None, year: int | None) -> pd.DataFrame:
    files = sorted(RAW_DART_DIR.glob("*_financials.csv"))
    if not files:
        raise FileNotFoundError("No OpenDART raw files found. Run quantify.dart_loader first.")

    frames: list[pd.DataFrame] = []
    for path in files:
        ticker = path.stem.replace("_financials", "")
        if tickers and ticker not in tickers:
            continue
        frame = pd.read_csv(path, dtype={"ticker": str, "bsns_year": int})
        if year is not None:
            frame = frame[frame["bsns_year"] == year]
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    ensure_output_dir()

    tickers = list(dict.fromkeys(args.tickers)) if args.tickers else None
    raw = load_raw_frames(tickers=tickers, year=args.year)
    if raw.empty:
        LOGGER.warning("No matching OpenDART raw records found for requested filters.")
        return

    output = to_annual_fundamentals(raw)
    output_path = PROCESSED_DIR / "fundamentals_annual.csv"
    output.to_csv(output_path, index=False, encoding="utf-8-sig")
    LOGGER.info("Saved %s rows to %s", len(output), output_path)


if __name__ == "__main__":
    main()
