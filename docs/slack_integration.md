# Slack Integration Guide

## Goal

Use Slack during operation to receive updates and support decision workflows
for buy/sell opinions with traceable rationale.

---

## 1) Prepare Webhook

1. Create a Slack App in your workspace.
2. Enable `Incoming Webhooks`.
3. Add a webhook to the target channel.
4. Store the webhook URL in local environment variable:

```powershell
$env:SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
```

For permanent user-level setup (Windows):

```powershell
setx SLACK_WEBHOOK_URL "https://hooks.slack.com/services/..."
```

Open a new terminal after `setx`.

---

## 2) Send a Test Message

```powershell
C:\Users\Chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m quantify.slack_notifier --title "Slack 연결 테스트" --message "Quantify Slack notifier is ready." --status SUCCESS --context "bootstrap"
```

Dry-run mode (no sending):

```powershell
C:\Users\Chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m quantify.slack_notifier --message "preview" --dry-run
```

---

## 3) Suggested Usage During Work

- Data collection start: job metadata + target range
- Data collection finish: success count, failure count, elapsed time
- Data quality warning: missing fields, unexpected null ratio
- Decision note: ticker, buy/sell opinion, key metrics, confidence, hold reason

---

## 4) Security Rules

- Never commit webhook URLs to git.
- Never send API keys or personal data in Slack messages.
- Keep detailed raw logs in local files; Slack should receive concise summaries.

