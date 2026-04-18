from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from quantify.slack_notifier import build_payload, load_config, send_webhook


LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
PRICE_DIR = ROOT_DIR / "data" / "raw" / "prices"
DEFAULT_WATCHLIST_PATH = ROOT_DIR / "config" / "watchlist.json"


@dataclass(frozen=True)
class HorizonResult:
    d21: float | None
    d63: float | None
    d126: float | None
    d252: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run watchlist monitoring and momentum screening in one task."
    )
    parser.add_argument(
        "--watchlist",
        default=str(DEFAULT_WATCHLIST_PATH),
        help="Path to watchlist json file.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of momentum candidates to report.",
    )
    parser.add_argument(
        "--min-history-days",
        type=int,
        default=260,
        help="Minimum rows required to include a ticker in momentum ranking.",
    )
    parser.add_argument(
        "--send-slack",
        action="store_true",
        help="Send result summary to Slack webhook.",
    )
    return parser.parse_args()


def load_watchlist(path: Path) -> list[str]:
    if not path.exists():
        LOGGER.warning("Watchlist file not found: %s", path)
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    tickers = payload.get("tickers", [])
    if not isinstance(tickers, list):
        raise ValueError("watchlist json must contain a 'tickers' list.")
    return [str(t).strip() for t in tickers if str(t).strip()]


def compute_horizon_returns(prices: pd.DataFrame) -> HorizonResult:
    series = prices["close"].astype(float).dropna().reset_index(drop=True)
    if series.empty:
        return HorizonResult(None, None, None, None)

    latest = series.iloc[-1]

    def ret(days: int) -> float | None:
        if len(series) <= days:
            return None
        past = series.iloc[-1 - days]
        if past == 0:
            return None
        return (latest / past - 1.0) * 100.0

    return HorizonResult(
        d21=ret(21),
        d63=ret(63),
        d126=ret(126),
        d252=ret(252),
    )


def score_momentum(h: HorizonResult) -> float | None:
    if h.d63 is None or h.d126 is None or h.d252 is None:
        return None
    return (0.4 * h.d63) + (0.3 * h.d126) + (0.3 * h.d252)


def load_price_file(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def build_watchlist_report(watchlist: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ticker in watchlist:
        path = PRICE_DIR / f"{ticker}.csv"
        if not path.exists():
            rows.append({"ticker": ticker, "status": "missing_price_file"})
            continue
        frame = load_price_file(path)
        h = compute_horizon_returns(frame)
        latest = frame.iloc[-1]
        rows.append(
            {
                "ticker": ticker,
                "status": "ok",
                "date": latest["date"].date().isoformat(),
                "close": float(latest["close"]),
                "ret_1m": h.d21,
                "ret_3m": h.d63,
                "ret_6m": h.d126,
                "ret_12m": h.d252,
            }
        )
    return pd.DataFrame(rows)


def build_momentum_ranking(min_history_days: int, top_n: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for path in sorted(PRICE_DIR.glob("*.csv")):
        frame = load_price_file(path)
        if len(frame) < min_history_days:
            continue
        ticker = path.stem
        h = compute_horizon_returns(frame)
        score = score_momentum(h)
        if score is None:
            continue
        latest = frame.iloc[-1]
        rows.append(
            {
                "ticker": ticker,
                "date": latest["date"].date().isoformat(),
                "close": float(latest["close"]),
                "ret_1m": h.d21,
                "ret_3m": h.d63,
                "ret_6m": h.d126,
                "ret_12m": h.d252,
                "momentum_score": score,
            }
        )

    ranking = pd.DataFrame(rows)
    if ranking.empty:
        return ranking
    ranking = ranking.sort_values("momentum_score", ascending=False).reset_index(drop=True)
    return ranking.head(top_n)


def fmt_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{value:+.2f}%"


def to_slack_message(watchlist_report: pd.DataFrame, ranking: pd.DataFrame) -> str:
    lines: list[str] = []
    lines.append("관찰 종목 모니터링")
    if watchlist_report.empty:
        lines.append("- 관찰 종목이 비어 있습니다.")
    else:
        for _, row in watchlist_report.iterrows():
            ticker = row["ticker"]
            if row.get("status") != "ok":
                lines.append(f"- {ticker}: 가격 데이터 없음")
                continue
            lines.append(
                f"- {ticker}: 종가 {row['close']:.0f}, 1M {fmt_pct(row['ret_1m'])}, "
                f"3M {fmt_pct(row['ret_3m'])}, 12M {fmt_pct(row['ret_12m'])}"
            )

    lines.append("")
    lines.append("모멘텀 상위 후보")
    if ranking.empty:
        lines.append("- 랭킹 산출 가능한 종목이 없습니다.")
    else:
        for i, (_, row) in enumerate(ranking.iterrows(), start=1):
            lines.append(
                f"- {i}. {row['ticker']} | score {row['momentum_score']:.2f} | "
                f"3M {fmt_pct(row['ret_3m'])}, 6M {fmt_pct(row['ret_6m'])}, 12M {fmt_pct(row['ret_12m'])}"
            )
    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    watchlist = load_watchlist(Path(args.watchlist))
    watchlist_report = build_watchlist_report(watchlist)
    ranking = build_momentum_ranking(args.min_history_days, args.top_n)

    message = to_slack_message(watchlist_report, ranking)
    print(message)

    if args.send_slack:
        payload = build_payload(
            title="관찰+모멘텀 병행 리포트",
            message=message,
            status="INFO",
            context="momentum-monitor",
        )
        config = load_config()
        send_webhook(config, payload)
        LOGGER.info("Slack report sent.")


if __name__ == "__main__":
    main()
