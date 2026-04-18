from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

try:
    from pykrx import stock
except ImportError as exc:  # pragma: no cover - handled at runtime
    stock = None
    PYKRX_IMPORT_ERROR = exc
else:
    PYKRX_IMPORT_ERROR = None

try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover - handled at runtime
    yf = None
    YFINANCE_IMPORT_ERROR = exc
else:
    YFINANCE_IMPORT_ERROR = None


LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
UNIVERSE_DIR = DATA_DIR / "universe"

MARKETS = ("KOSPI", "KOSDAQ")
KIND_DOWNLOAD_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do"
KIND_MARKET_TYPES = {
    "KOSPI": "stockMkt",
    "KOSDAQ": "kosdaqMkt",
}


@dataclass(frozen=True)
class LoaderConfig:
    start_date: str
    end_date: str
    throttle_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Korean stock universe metadata and daily OHLCV data."
    )
    parser.add_argument(
        "--start-date",
        default=(date.today() - timedelta(days=365 * 5)).strftime("%Y%m%d"),
        help="Start date in YYYYMMDD format. Default is 5 years ago.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().strftime("%Y%m%d"),
        help="End date in YYYYMMDD format. Default is today.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        help="Optional list of ticker codes. If omitted, collect all KOSPI/KOSDAQ tickers.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit after filtering tickers. Useful for quick smoke tests.",
    )
    parser.add_argument(
        "--throttle-seconds",
        type=float,
        default=0.0,
        help="Optional sleep interval between ticker requests.",
    )
    return parser.parse_args()


def ensure_dependencies() -> None:
    if yf is None:
        raise RuntimeError(
            "yfinance is required for price collection fallback. Install dependencies first.\n"
            f"Original import error: {YFINANCE_IMPORT_ERROR}"
        )


def normalize_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYYMMDD.") from exc


def ensure_directories() -> None:
    for path in (RAW_DIR / "prices", UNIVERSE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def get_market_universe(as_of: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for market in MARKETS:
        response = requests.get(
            KIND_DOWNLOAD_URL,
            params={
                "method": "download",
                "searchType": 13,
                "marketType": KIND_MARKET_TYPES[market],
            },
            timeout=30,
        )
        response.raise_for_status()
        frame = pd.read_html(StringIO(response.text), header=0)[0]
        frame = frame.rename(
            columns={
                "종목코드": "ticker",
                "회사명": "name",
                "상장일": "listed_at",
            }
        )
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame["market"] = market
        keep_columns = [column for column in ("ticker", "name", "market", "listed_at") if column in frame.columns]
        frames.append(frame[keep_columns])

    universe = pd.concat(frames, ignore_index=True)
    universe["collected_at"] = pd.Timestamp.utcnow()
    universe = universe.sort_values(["market", "ticker"]).reset_index(drop=True)
    return universe


def get_price_frame_from_pykrx(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    if stock is None:
        return pd.DataFrame()

    try:
        frame = stock.get_market_ohlcv_by_date(start_date, end_date, ticker, adjusted=True)
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        LOGGER.warning("pykrx price fetch failed for %s: %s", ticker, exc)
        return pd.DataFrame()

    frame = frame.reset_index().rename(
        columns={
            "날짜": "date",
            "시가": "open",
            "고가": "high",
            "저가": "low",
            "종가": "close",
            "거래량": "volume",
            "거래대금": "turnover",
            "등락률": "return",
        }
    )
    return normalize_price_frame(frame, ticker)


def get_price_frame_from_yfinance(ticker: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    symbol = f"{ticker}{suffix}"
    start = datetime.strptime(start_date, "%Y%m%d").date().isoformat()
    end = (datetime.strptime(end_date, "%Y%m%d").date() + timedelta(days=1)).isoformat()

    frame = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=False)
    if frame.empty:
        return frame

    frame = frame.reset_index()
    frame.columns = [column[0] if isinstance(column, tuple) else column for column in frame.columns]
    frame = frame.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    frame["turnover"] = pd.NA
    frame["return"] = frame["close"].pct_change().mul(100)
    return normalize_price_frame(frame, ticker)


def normalize_price_frame(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if frame.empty:
        return frame

    frame.columns = [str(column).lower() for column in frame.columns]
    frame["ticker"] = ticker
    frame["date"] = pd.to_datetime(frame["date"])
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "turnover",
        "return",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def get_price_frame(ticker: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
    pykrx_frame = get_price_frame_from_pykrx(ticker, start_date, end_date)
    if not pykrx_frame.empty:
        return pykrx_frame
    return get_price_frame_from_yfinance(ticker, market, start_date, end_date)


def save_universe(universe: pd.DataFrame, as_of: str) -> Path:
    output_path = UNIVERSE_DIR / f"universe_{as_of}.csv"
    universe.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def save_prices(prices: pd.DataFrame, ticker: str) -> Path | None:
    if prices.empty:
        return None
    output_path = RAW_DIR / "prices" / f"{ticker}.csv"
    prices.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def select_tickers(universe: pd.DataFrame, requested: Iterable[str] | None, limit: int | None) -> list[str]:
    if requested:
        requested_set = set(requested)
        available = set(universe["ticker"].tolist())
        missing = sorted(requested_set - available)
        if missing:
            raise ValueError(f"Unknown tickers: {', '.join(missing)}")
        tickers = [ticker for ticker in universe["ticker"].tolist() if ticker in requested_set]
    else:
        tickers = universe["ticker"].tolist()

    if limit is not None:
        tickers = tickers[:limit]
    return tickers


def run_collection(config: LoaderConfig, requested_tickers: Iterable[str] | None, limit: int | None) -> None:
    ensure_dependencies()
    ensure_directories()

    LOGGER.info("Collecting market universe for %s", config.end_date)
    universe = get_market_universe(config.end_date)
    universe_path = save_universe(universe, config.end_date)
    LOGGER.info("Saved universe snapshot: %s", universe_path)

    tickers = select_tickers(universe, requested_tickers, limit)
    LOGGER.info("Collecting prices for %s tickers", len(tickers))

    for index, ticker in enumerate(tickers, start=1):
        market = universe.loc[universe["ticker"] == ticker, "market"].iloc[0]
        name = universe.loc[universe["ticker"] == ticker, "name"].iloc[0]
        LOGGER.info("[%s/%s] Fetching %s %s", index, len(tickers), ticker, name)
        prices = get_price_frame(ticker, market, config.start_date, config.end_date)
        output_path = save_prices(prices, ticker)
        if output_path is None:
            LOGGER.warning("No price data returned for %s", ticker)
            continue
        LOGGER.info("Saved %s rows to %s", len(prices), output_path)
        if config.throttle_seconds > 0:
            import time

            time.sleep(config.throttle_seconds)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = parse_args()
    config = LoaderConfig(
        start_date=normalize_date(args.start_date),
        end_date=normalize_date(args.end_date),
        throttle_seconds=args.throttle_seconds,
    )
    run_collection(config, args.tickers, args.limit)


if __name__ == "__main__":
    main()
