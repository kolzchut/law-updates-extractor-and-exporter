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