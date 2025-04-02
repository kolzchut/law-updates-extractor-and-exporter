import logging

import requests
import urllib3


urllib3.disable_warnings()


logger = logging.getLogger(__name__)


def get_html(source, limit=10, skip=0):
    url = 'https://pub-justice.openapi.gov.il/pub/moj/portal/rest/searchpredefinedapi/v1/SearchPredefinedApi/Reshumot/Search'
    folder_types = {
        'laws': "1",
        'notifications': "2",
        'takanot': "3"
    }

    # This is the key used by the gov.il website currently
    headers = {'x-client-id': '149a5bad-edde-49a6-9fb9-188bd17d4788'}

    data = {
        "skip": skip,
        "limit": str(limit),
        "FolderType": folder_types[source]
    }

    res = requests.post(url, json=data, headers=headers)

    if res.status_code == 200:
        return res.json()
    logger.error(f"We didn't get 200 from {source}, we got {res.status_code}")
    raise SystemExit(f'We got {res.status_code} from {source}')
