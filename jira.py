import base64
import logging
import os

import requests


logger = logging.getLogger(__name__)


class JiraApi:
    def __init__(self):
        self.url = 'https://kolzchut.atlassian.net/rest/api/2/issue/'
        self.user_name = 'moshegrey@gmail.com'
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
            payload = {
                'fields': {
                    'project': {
                        'key': 'KOL',
                    },
                    'summary': datum['display_name'],
                    'description': datum['description'],
                    'issuetype': {
                        'name': 'שינוי חקיקה (עברית)',
                    },
                    'reporter': self.user_name,
                    'customfield_11690': datum['published_date'],
                    'customfield_11689': datum['file_name'],
                }
            }
            logger.info(f'headers: {self.headers}')
            res = requests.post(self.url, headers=self.headers, json=payload)

            if res.status_code >= 300:
                logger.error(f'status code: {res.status_code}')
                logger.error(f'result: {res.content}')
                break
            else:
                logger.info(f'status code: {res.status_code}')
                logger.info(f'result: {res.content}')
