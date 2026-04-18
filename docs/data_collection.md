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

## OpenDART Financials

Download the OpenDART corporation code mapping and annual major accounts:

```bash
python -m quantify.dart_loader --api-key YOUR_KEY --tickers 005930 000660 --start-year 2020
```

You can also set the key through `OPEN_DART_API_KEY` and omit `--api-key`.

Output:

- `data/universe/dart_corp_codes.csv`
- `data/raw/opendart/<ticker>_financials.csv`
- Slack notifications for start/success/failure are sent when `SLACK_WEBHOOK_URL` is configured.

Notes:

- the implementation uses the official `corpCode.xml` download to map stock tickers
  to DART `corp_code`
- the financial endpoint currently uses the official single-company major accounts API
  for annual, half-year, and quarterly report requests

---

## Fundamentals Processing

Convert raw OpenDART statements into annual factor-ready fundamentals:

```bash
python -m quantify.financials_processor
```

Optional filters:

```bash
python -m quantify.financials_processor --tickers 005930 000660 --year 2024
```

Output:

- `data/processed/fundamentals_annual.csv`

Key columns:

- `assets_total`, `liabilities_total`, `equity_total`
- `revenue`, `operating_income`, `net_income`
- `debt_ratio`, `roe`

---

## Output

- `data/universe/universe_YYYYMMDD.csv`
- `data/raw/prices/<ticker>.csv`

---

## Next Step

Once this base layer is stable, the next addition should be fundamental data
from OpenDART so that value and quality factors can be calculated with proper
point-in-time handling.
