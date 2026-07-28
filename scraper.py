import logging

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


urllib3.disable_warnings()


logger = logging.getLogger(__name__)


# The Ministry of Justice API drops connections every so often, and a single
# refused connect used to fail the whole nightly run (see issue #3). Retry the
# fetch a handful of times before giving up.
#
# Scoped to this module on purpose: retrying the *search* POST is safe because
# it only reads, whereas the deployment runs the job with replicaRetryLimit=0
# precisely because re-running the whole scraper could double-post to Jira.
#
# POST has to be opted into `allowed_methods` explicitly — urllib3 leaves it
# out by default (it assumes POST writes something) and without it a mid-
# request "connection reset by peer" is re-raised instead of retried.
#
# `raise_on_status=False` keeps a retried-out HTTP status coming back as a
# normal response, so it lands on the status check below and still fails with
# a readable message rather than a urllib3 MaxRetryError.
_RETRY = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    allowed_methods=frozenset({'POST'}),
    status_forcelist=(429, 502, 503, 504),
    # Sleeps 0s, 4s, 8s, 16s, 32s (urllib3 skips the backoff on the first
    # retry, then doubles from backoff_factor * 2) — 60s per fetch worst case,
    # so ~180s across the three fetches. Comfortably inside the job's 1800s
    # replica timeout even if every fetch has to exhaust its retries.
    backoff_factor=2,
    raise_on_status=False,
)

_session = requests.Session()
# Mounted on both schemes so the retry policy can't silently go missing if the
# URL ever changes — an adapter only applies to the prefix it is mounted on.
_adapter = HTTPAdapter(max_retries=_RETRY)
_session.mount('https://', _adapter)
_session.mount('http://', _adapter)

# (connect, read). Without a timeout a hung connection blocks until the job's
# replica timeout kills it — and a request that never returns never triggers a
# retry either, which would defeat the whole point.
_TIMEOUT = (10, 60)


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

    # A transient fault is retried in the adapter; anything that survives all
    # of them raises requests.ConnectionError, which the caller turns into a
    # run_summary with status=error. Real outages still fail the run loudly.
    res = _session.post(url, json=data, headers=headers, timeout=_TIMEOUT)

    if res.status_code == 200:
        return res.json()
    logger.error(f"We didn't get 200 from {source}, we got {res.status_code}")
    raise SystemExit(f'We got {res.status_code} from {source}')
