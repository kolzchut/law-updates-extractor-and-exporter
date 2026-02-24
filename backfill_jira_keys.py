#!/usr/bin/env python3
"""
One-off script to backfill jira_key for existing DB records.

For each row with no jira_key, searches Jira by file_name (customfield_11689)
and reports what was found or is missing.

Runs in dry-run mode by default; pass --fix to actually write keys to the DB.
"""

import argparse
import logging
import time

import database
from jira import JiraApi


logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--fix', action='store_true',
        help='Write found Jira keys to the DB (default: report only)'
    )
    parser.add_argument(
        '--from-booklet', type=int, metavar='BOOKLET_NUMBER',
        help='Only process records with booklet_number >= this value'
    )
    parser.add_argument(
        '--limit', type=int,
        help='Process at most this many booklets (useful for testing)'
    )
    parser.add_argument('--log', default='info')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log.upper()))

    jira = JiraApi()

    with database.Database() as db:
        items = db.get_all_without_jira_key(from_booklet=args.from_booklet)
        logger.info(
            f'{len(items)} DB record(s) have no jira_key'
            + (f' (from booklet #{args.from_booklet})' if args.from_booklet else '')
        )
        if args.limit:
            seen_nums = set()
            limited = []
            for item in items:
                seen_nums.add(item['booklet_number'])
                limited.append(item)
                if len(seen_nums) >= args.limit:
                    break
            items = limited
            logger.info(f'limiting to {args.limit} booklet(s) ({len(items)} row(s))')

        found = []
        missing = []

        for item in items:
            booklet_num = item['booklet_number']
            display_name = item['display_name']
            jira_key = jira.search_by_display_name(display_name, published_date=item.get('published_date'))
            if jira_key:
                logger.info(f'  #{booklet_num} ({display_name}): found {jira_key}')
                found.append((item, jira_key))
                if args.fix:
                    db.update_jira_key_by_id(item['id'], jira_key)
            else:
                logger.info(f'  #{booklet_num} ({display_name}): not found in Jira')
                missing.append(booklet_num)

            time.sleep(0.2)  # avoid hammering the Jira API

        missing_unique = sorted(set(missing))
        logger.info(
            f'\nSummary: {len(found)} row(s) found in Jira, '
            f'{len(set(missing))} booklet(s) not found ({len(missing)} row(s))'
        )
        if missing_unique:
            logger.info(f'Not in Jira: {", ".join(str(n) for n in missing_unique)}')
        if not args.fix and found:
            logger.info('Re-run with --fix to write the keys to the DB')


if __name__ == '__main__':
    main()
