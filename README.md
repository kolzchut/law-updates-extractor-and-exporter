# laws-fixes-extractor-and-exporter
A project done for Kol Zchut

## Configuration
There are two required environment variables:
- `JIRA_API_USER`: the email address of the user that will be used for Jira API
- `JIRA_API_TOKEN`: the token for said user

## Script parameters
- `--last-booklet`, `-l`: override the last booklet number stored in the sqlite DB and get all updates since this one