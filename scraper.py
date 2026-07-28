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
    # retry, then doubles from backoff_factor * 2) — 60s of backoff per fetch.
    #
    # Backoff is only part of the worst case: each of the 6 attempts can also
    # burn its full timeout, so a fetch can take 6 * (10 + 30) + 60 = ~300s,
    # and a run ~900s across the three fetches. That still leaves half of the
    # job's 1800s replica timeout for the Jira posting that follows.
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
#
# The read budget is deliberately tight: a full 500-item fetch measures 1-4s
# against the live API, so 30s is ~10x headroom. Keeping it low matters because
# the timeout is per attempt and there are 6 of them — a generous value
# multiplies into the run's worst case (see the retry budget above).
_TIMEOUT = (10, 30)


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
    # A plain Exception, not SystemExit: SystemExit derives from BaseException,
    # so main()'s `except Exception` would not catch it and the run would end
    # without its run_summary — leaving the failure alert with no reason to
    # report. That mattered little while this branch was near-unreachable, but
    # retries now route every persistent 429/502/503/504 straight through it.
    raise RuntimeError(f'We got {res.status_code} from {source}')
