import base64
import logging
import os
import time

import requests


logger = logging.getLogger(__name__)


class JiraApi:
    def __init__(self):
        self.url = 'https://kolzchut.atlassian.net/rest/api/2/issue/'
        self.user_name = os.environ['JIRA_API_USER']
        api_token = os.environ['JIRA_API_TOKEN']
        token = f'{self.user_name}:{api_token}'
        bytes_token = bytes(token, encoding='utf8')
        token = str(base64.b64encode(bytes_token).decode('utf8'))
        self.headers = {
            'authorization': f'Basic {token}',
        }

    def send(self, data, dry_run=False):
        """Send items to Jira. Returns list of (datum, jira_key) for each successfully created issue."""
        results = []
        for datum in data:
            summary = datum['display_name'] if len(datum['display_name']) < 255 else f"{datum['display_name'][:250]}..."
            payload = {
                'fields': {
                    'project': {
                        'key': 'KOL',
                    },
                    'summary': summary,
                    'description': datum['description'],
                    'issuetype': {
                        'name': 'שינוי חקיקה (עברית)',
                    },
                    'reporter': self.user_name,
                    'customfield_11690': datum['published_date'],
                    'customfield_11689': datum['file_name'],
                    'customfield_11703': datum['display_name'],
                }
            }
            booklet_num = datum.get('booklet_number', '?')
            logger.info(f'  sending #{booklet_num}: {summary[:80]}')
            logger.debug(f'  full payload for #{booklet_num}: {payload}')
            if dry_run:
                print(f'[DRY RUN] Jira issue payload:\n{payload}')
                continue
            res = requests.post(self.url, headers=self.headers, json=payload)

            if res.status_code >= 300:
                logger.error(f'  #{booklet_num} failed – status {res.status_code}: {res.content}')
                logger.error(f'  file: {datum["file_name"]}')
                break
            else:
                jira_key = res.json().get('key')
                logger.info(f'  #{booklet_num} → {jira_key}')
                results.append((datum, jira_key))
        return results

    @staticmethod
    def _jql_escape(value):
        """Escape a value for use inside a JQL double-quoted string."""
        value = value.replace('\\', '\\\\')   # must be first
        value = value.replace('"', '\\"')
        value = value.replace('\t', '\\t')
        value = value.replace('\n', '\\n')
        return value

    @staticmethod
    def _normalize_summary(value):
        """Normalize whitespace so DB values and Jira-indexed values can be compared."""
        import re
        return re.sub(r'\s+', ' ', value).strip()

    def _search_jql(self, jql, fields, max_results=50):
        """Execute a JQL search, handling rate limiting. Returns list of issues or None on error."""
        params = {'jql': jql, 'maxResults': max_results, 'fields': fields}
        res = requests.get(
            'https://kolzchut.atlassian.net/rest/api/3/search/jql',
            headers=self.headers,
            params=params,
        )
        while res.status_code == 429:
            wait = int(res.headers.get('Retry-After', 10))
            logger.warning(f'Rate limited by Jira, waiting {wait}s...')
            time.sleep(wait)
            res = requests.get(
                'https://kolzchut.atlassian.net/rest/api/3/search/jql',
                headers=self.headers,
                params=params,
            )
        if res.status_code != 200:
            logger.error(f'Jira search failed: {res.status_code} {res.content}')
            return None
        return res.json().get('issues', [])

    def search_by_file_name(self, file_name):
        """Search Jira for an issue by file URL (customfield_11689).
        Uses exact JQL match on cf[11689], which is unique per issue.
        Returns the issue key string, or None if not found."""
        jql = f'project = KOL AND cf[11689] = "{self._jql_escape(file_name)}"'
        issues = self._search_jql(jql, fields='summary', max_results=2)
        if issues is None:
            return None
        if len(issues) == 1:
            return issues[0]['key']
        if len(issues) > 1:
            keys = [i['key'] for i in issues]
            logger.warning(f'Multiple Jira issues match file_name {file_name!r}: {keys}')
        return None

    def search_by_display_name(self, display_name, published_date=None):
        """Search Jira for an issue whose summary matches display_name.
        Uses ~ (contains) because Jira normalizes tabs during indexing, then
        post-filters results by normalized whitespace comparison.
        If multiple summaries match and published_date is given, uses
        customfield_11690 to disambiguate.
        Returns the issue key string, or None if not found (or multiple found)."""
        # Use the text before any tab as the search term (page numbers after \t are noise)
        search_term = display_name.split('\t')[0].strip()
        # Hebrew year notation (e.g. התשפ"ו) uses ASCII " which breaks JQL ~ queries;
        # truncate at the first " since the title before it is unique enough for searching.
        # The post-filter on the full display_name catches any false positives.
        if '"' in search_term:
            search_term = search_term.split('"')[0].strip().rstrip(',').strip()
        jql = f'project = KOL AND summary ~ "{self._jql_escape(search_term)}"'
        issues = self._search_jql(jql, fields='summary,customfield_11690')
        if issues is None:
            return None
        if not issues:
            return None
        # Post-filter: normalize whitespace and compare to our expected summary
        target = self._normalize_summary(display_name)
        matches = [i for i in issues if self._normalize_summary(i['fields']['summary']) == target]
        if len(matches) == 1:
            return matches[0]['key']
        if len(matches) > 1 and published_date:
            date_matches = [i for i in matches
                            if i['fields'].get('customfield_11690') == published_date]
            if len(date_matches) == 1:
                return date_matches[0]['key']
            if len(date_matches) > 1:
                keys = [i['key'] for i in date_matches]
                logger.warning(f'Multiple Jira issues match {display_name!r} + date {published_date}: {keys}')
                return None
        if len(matches) > 1:
            keys = [i['key'] for i in matches]
            logger.warning(f'Multiple Jira issues match {display_name!r}: {keys}')
            return None
        return None
