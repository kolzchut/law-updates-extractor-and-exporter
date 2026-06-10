# laws-fixes-extractor-and-exporter
A project done for Kol Zchut

## Configuration
Required environment variables:
- `JIRA_API_USER`: the email address of the user that will be used for Jira API
- `JIRA_API_TOKEN`: the token for said user

Optional environment variables (sensible defaults for Kol-Zchut):
- `JIRA_API_URL`: Jira base URL (default `https://kolzchut.atlassian.net`)
- `JIRA_PROJECT`: Jira project key (default `KOL`)
- `LAW_UPDATES_STATE_DIR`: directory holding `kzdb.sqlite` (default: current
  directory). In the Container Apps Job this is the mounted state share.
- `LAW_UPDATES_DB_PATH`: full path to the SQLite file; overrides
  `LAW_UPDATES_STATE_DIR` when set.

## State database
The SQLite state DB (`kzdb.sqlite`) is **not** committed to this repo — it is
the runtime source of truth and lives on the deployment's state share. Seed it
from the production copy before the first run. The app **refuses to run**
against a DB with no `booklet` table: an empty DB has no anchor, so every
fetched item would look new and flood Jira with duplicates.

## Script parameters
- `--last-takana`, `-t`: override the last takana number stored in the sqlite DB and get all updates since this one
- `--last-law`, `-l`: override the last law number stored in the sqlite DB and get all updates since this one
- `--last-notification`, `-n`: override the last notification number stored in the sqlite DB and get all updates since this one
- `--log`: enable to change the log level; possible parameters are:
  - debug, info, warning, error
- `--log-format`: `text` (default, human-readable) or `json` (one JSON object
  per line). The scheduled Container Apps Job runs with `--log-format json` so
  Azure Log Analytics can query individual fields.

## Structured logging
With `--log-format json` every log line is a JSON object (`time`, `level`,
`logger`, `message`, plus any structured fields). At the end of every run the
scraper emits exactly one **`run_summary`** event:

```json
{"time": "...", "level": "INFO", "logger": "__main__", "message": "run complete",
 "event": "run_summary",
 "summary": {"status": "ok", "dry_run": false,
             "counts": {"laws": 2, "regulations": 0, "notifications": 1, "total": 3},
             "jira_keys": ["KOL-101", "KOL-102", "KOL-103"]}}
```

`summary.status` is `"error"` (with a `summary.error` message) if any item
failed to post to Jira or the run hit an unhandled exception — the deployment's
failure alert pivots on this field.