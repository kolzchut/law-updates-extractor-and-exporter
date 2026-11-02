# AGENTS.md

Notes for coding agents working on this repo. Read `README.md` first: it
covers the environment variables, the state DB seeding rule, the `--last-*`,
`--log` and `--log-format` flags, the `run_summary` event and the upstream
API retry budget. This file adds what the README does not say.

## How a run flows

`main.py` drives one pipeline per booklet type (laws, regulations,
notifications):

1. `scraper.get_html()` POSTs to the Ministry of Justice Reshumot search API
   and returns the raw JSON (up to 500 entries per type).
2. `cleaner.clean_data()` turns each API result into one or more row dicts.
3. `main.py` de-duplicates the batch, drops rows already in the DB or older
   than the anchor window, and inserts the rest (`database.Database`).
4. `jira.JiraApi.send()` creates one Jira issue per inserted row, and
   `main.py` writes each returned key back with `update_jira_key_by_id()`.

`backfill_jira_keys.py` is a separate maintenance script (see below).
`logsetup.py` holds the text and JSON log formatters.

## Deployment

The scheduled job is not deployed from this repo. kz-infrastructure builds the
image (`docker/law-updates-scraper/`) from a commit of this repo pinned in its
`ansible/ca-job-source-pins.yml`, and its `law_updates_job` Ansible role
deploys the Container Apps Job. Only the Jira API token is a secret, held in
that repo's vault. The Jira user is plain inventory config. So a merge here changes nothing in production until the pin moves and
a release ships. The state DB is not committed here (see README).

## State DB (`booklet` table)

- `booklet_type` in the DB: 1 = law, 2 = takana (regulation),
  3 = notification (`Database.booklet_types`). The API's `FolderType` uses a
  **different** order: laws 1, notifications 2, takanot 3 (`scraper.py`).
  Don't cross the two maps.
- One booklet (one PDF) usually yields several rows. `clean_data()` emits one
  row per `<br/>`-separated line of the booklet's summary, except when the
  summary contains an indirect-amendment marker (`תיקונים עקיפים:` or
  `תיקון עקיף:`), which yields a single row.
- A row's identity is `(booklet_number, display_name)` within its type. The
  in-batch dedup and the "already in DB" check both use it. `file_name` is the
  PDF URL, and every row of a booklet shares it, so it does not identify a row.
  Each row gets its own Jira issue.
- Booklet numbers are separate series per type (in the 2021 data, law
  booklets were near 2,900 and regulation booklets near 9,700). Any query
  that filters by `booklet_number` without `booklet_type` mixes types.
- `jira_key` is added by `Database.__enter__` if the column is missing. NULL
  means the row was never posted or its key was never recorded. Record keys
  per row with `update_jira_key_by_id()`. `update_jira_key(booklet_number,
  booklet_type, ...)` writes every row of the booklet, so it is only a
  fallback.
- The anchor per type is the **last inserted** row (`ORDER BY id DESC`), not
  the highest booklet number. A fetched row is accepted when it is not already
  stored and its number is at least `anchor - lookback`.

## CLI behaviour the README doesn't cover

`main.py --help` is authoritative. The behaviour that matters:

- `--lookback N` (default 50): how far behind the anchor to accept rows, to
  fill gaps the API skipped on an earlier run.
- `--dry-run`: prints one line per would-be insert and a "would send N
  item(s) to Jira" count. It builds no Jira payloads (only `--resend
  --dry-run` prints those) and inserts nothing. Opening the DB can still add
  the `jira_key` column (see above), so the file is not strictly untouched.
- `--last-law/--last-takana/--last-notification N` look up that exact booklet
  number. If it is not in the DB there is no anchor, and every fetched row not
  already stored counts as new. Run with `--dry-run` first.
- `--resend N` re-posts every DB row whose `booklet_number` is N, across all
  types, including rows that already have an issue. It can create duplicate
  issues. Despite its help text it does write the new keys to those rows (in
  `--dry-run` it writes nothing).

## Jira

- Issues are created through REST v2 (`/rest/api/2/issue/`). Search uses v3
  (`/rest/api/3/search/jql`) because the v2 search endpoint returns 410.
  That was observed against Jira by February 2026 and is not shown in the
  code.
- `send()` maps fields as follows. The summary is `display_name`, cut to 250
  characters plus `...` when it is 255 or longer. `customfield_11690` is
  `published_date`, `customfield_11689` is `file_name` and
  `customfield_11703` is `display_name`. The reporter is the API user.
- `send()` stops at the first failed POST. The rows after it are already in
  the DB with no `jira_key`, and the next run skips them because they are
  stored. Recover with `--resend`, mindful of the duplicates it can make.
- POSTs are not retried. `_search_jql()` retries 429 responses, sleeping for
  `Retry-After` each time, with no cap.

### Search quirks (`search_by_display_name`)

- `summary = "..."` misses values that contain tabs, because Jira normalises
  whitespace when indexing. Search with `summary ~` on the text before the
  first tab, then post-filter with `_normalize_summary()`.
- Hebrew year notation such as `התשפ"ו` uses an ASCII `"`, which breaks a
  JQL `~` query. The search term is cut at the first `"`.
  The post-filter still compares the full `display_name`.
- If several summaries match, `customfield_11690` (published date)
  disambiguates. If it is still ambiguous, the method returns None and logs
  a warning.
- In v3, `cf[11703]` (display_name) comes back as ADF rich text, not a plain
  string, so don't search or compare on it. This was observed by February
  2026 and is not shown in the code.
- `search_by_file_name()` exact-matches `cf[11689]`. Because `file_name` is
  shared by every row of a booklet, it returns None for multi-row booklets.
  That is why the backfill searches by display name, although its module
  docstring still says file name.

## Backfilling `jira_key`

`backfill_jira_keys.py` looks up rows with a NULL `jira_key` in descending
`booklet_number` order across all types, with a 0.2s pause between searches. It only reports unless you pass
`--fix`. `--limit N` counts distinct booklet numbers, not rows.

`--from-booklet N` filters on `booklet_number` without regard to type. Rows
from before the Jira integration have no issue to find. The oldest booklet
with an issue is regulation 9780 (observed in Jira by February 2026, not
shown in the code), so `--from-booklet 9780` is the right floor
for regulations. Law booklet numbers are far lower, though, so that floor
skips every law. To backfill laws, run without the flag or with a law-series
floor.

Because the order ignores type and law numbers are the lowest, a
`--limit 20` sample holds only regulations and notifications. Recipe:

1. Run report-only with `--limit 20` and check the matches.
2. Spot-check laws as well, for example with a law-series `--from-booklet`
   or by looking up a few law rows by hand.
3. Repeat with `--fix`.

## Conventions

- User-facing log text says "regulations". The code's identifiers keep
  `takana`/`takanot`, so don't rename them.
- A failure must still reach `emit_run_summary()`. Raise an `Exception`
  subclass, not `SystemExit`, inside a run (see the comment in `scraper.py`).
  Two existing paths break this rule:
  - The DB guards in `Database.__enter__` (missing file, no `booklet` table)
    raise `SystemExit` with a message. They skip the summary, but they exit
    non-zero, so the deployment's `job_failed` marker still fires.
  - `clean_data()` calls a bare `exit()` when a result has more than one
    document. That exits 0 with no summary. The kz-infrastructure entrypoint
    emits `job_failed` only on a non-zero exit, so the execution looks
    successful. The bug is tracked in #8.
