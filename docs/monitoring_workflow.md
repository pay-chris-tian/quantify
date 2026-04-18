# Monitoring Workflow

## Goal

Run two tasks in parallel:

1. watchlist monitoring for currently observed stocks
2. momentum screening for new candidates

---

## 1) Watchlist Setup

Create `config/watchlist.json` from the example:

```powershell
Copy-Item config\watchlist.example.json config\watchlist.json
```

Edit `tickers` in `config/watchlist.json` whenever you want to change observed stocks.

---

## 2) Data Preparation

Update price data for watchlist stocks:

```powershell
C:\Users\Chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m quantify.data_loader --tickers 377300 005930 000660 --start-date 20220101
```

---

## 3) Parallel Monitoring + Momentum Screening

Run analysis report:

```powershell
C:\Users\Chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m quantify.momentum_monitor --watchlist config\watchlist.json --top-n 10
```

Send the report to Slack:

```powershell
C:\Users\Chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m quantify.momentum_monitor --watchlist config\watchlist.json --top-n 10 --send-slack
```

---

## 4) Output Meaning

- Watchlist section: status + short-term and medium-term returns of observed stocks
- Momentum section: top ranked candidates based on weighted score
  - score = 0.4 * 3M + 0.3 * 6M + 0.3 * 12M

---

## 5) Documentation Rule

- Record each run in `docs/work_log.md`
- Keep decision rationale in both document and Slack message
