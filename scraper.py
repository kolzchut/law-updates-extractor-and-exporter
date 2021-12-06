import logging

import requests
import urllib3


urllib3.disable_warnings()


logger = logging.getLogger(__name__)


def get_html(source, limit=10, skip=0):
    url = 'https://rfa.justice.gov.il/SearchPredefinedApi/Reshumot/Search'
    sources = {
        'laws': {'skip': skip, 'limit': limit, 'FolderType': "1"},
        'takanot': {'skip': skip, 'limit': limit, 'FolderType': "3"},
    }
    res = requests.post(url, json=sources[source], verify=False)

    if res.status_code == 200:
        return res.json()
    logger.error(f"We didn't get 200 from {source}, we got {res.status_code}")
    raise SystemExit(f'We got {res.status_code} from {source}')
