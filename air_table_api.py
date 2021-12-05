import logging
import os
import time

import requests


logger = logging.getLogger(__name__)


class AirTable:
    def __init__(self):
        self.token = os.environ.get('AIRTABLE_TOKEN_API')
        self.types = {
            'takana': 'תקנה',
            'law': 'חוק',
        }
        self.url = 'https://api.airtable.com/v0/app9yQhanoA92YZXu/Table%201'

    def _create_batches_of_ten(self, data):
        inner_list = []
        outer_list = []
        idx = 1
        for idx, datum in enumerate(data, 1):
            inner_list.append(datum)
            if idx % 10 == 0:
                outer_list.append(list(inner_list))
                inner_list = []
        if idx % 10 != 0 and inner_list:
            outer_list.append(inner_list)
        return outer_list

    def _send_batch(self, data):
        res = requests.post(self.url, headers={'authorization': f'Bearer {self.token}'}, json=data)
        logger.info(res.status_code)

    def create_records(self, data):
        logger.info(data)
        data = self._create_batches_of_ten(data)
        logger.info(data)
        data_to_send = []
        for ten in data:
            for one in ten:
                d = {
                    'Name': one['display_name'],
                    'Type': self.types[one['booklet_type']],
                    'Description': one['description'],
                    'File of booklet': one['file_name'],
                    'Status': 'Todo',
                }
                data_to_send.append({'fields': d})
            self._send_batch({'records': data_to_send})
            data_to_send = []
            time.sleep(0.5)
