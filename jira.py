import base64
import logging
import os

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
        logger.info(f'token: {token}')
        self.headers = {
            'authorization': f'Basic {token}',
        }

    def send(self, data):
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
            logger.info(f'headers: {self.headers}')
            res = requests.post(self.url, headers=self.headers, json=payload)

            if res.status_code >= 300:
                logger.error(f'status code: {res.status_code}')
                logger.error(f'result: {res.content}')
                logger.error(f'the file that failed is: {datum["file_name"]}')
                break
            else:
                logger.info(f'status code: {res.status_code}')
                logger.info(f'result: {res.content}')
