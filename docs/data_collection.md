# Data Collection

## Scope

The current collector focuses on the minimum data needed to start a backtest:

- universe snapshot for KOSPI and KOSDAQ
- daily OHLCV price history for each ticker

The implementation uses:

- `KIND` downloadable market list for universe metadata
- `pykrx` as the first price source
- `yfinance` as a fallback when `pykrx` is unavailable or unstable

---

## Install

```bash
pip install -r requirements.txt
```

---

## Run

Collect the full market universe and five years of daily prices:

```bash
python -m quantify.data_loader
```

Collect a small sample first:

```bash
python -m quantify.data_loader --tickers 005930 000660 --start-date 20230101
```

Collect only the first 20 tickers for a smoke test:

```bash
python -m quantify.data_loader --limit 20
```

---

## Output

- `data/universe/universe_YYYYMMDD.csv`
- `data/raw/prices/<ticker>.csv`

---

## Next Step

Once this base layer is stable, the next addition should be fundamental data
from OpenDART so that value and quality factors can be calculated with proper
point-in-time handling.
