#!/usr/bin/env python

import argparse
import logging

from cleaner import clean_data
import database
from scraper import get_html
from jira import JiraApi


logger = logging.getLogger(__name__)

DEFAULT_FETCH_LIMIT = 500
DEFAULT_LOOKBACK = 50


def should_insert_booklet(last_booklet, booklet, existing_numbers: set, lookback: int):
    """
    Return True if this booklet should be inserted into the DB.

    Rules:
    - Skip if already in the DB (prevents duplicates).
    - If no last_booklet exists, treat everything as new.
    - Otherwise accept anything within `lookback` items behind the last known
      number, so that gaps (items the API skipped on a previous run) are
      back-filled, as well as anything newer than the last known number.
    """
    booklet_num = int(booklet['booklet_number'])
    label = booklet.get('booklet_type', '?')

    if booklet_num in existing_numbers:
        logger.debug(f'  skip {label} #{booklet_num}: already in DB')
        return False

    if not last_booklet:
        logger.debug(f'  insert {label} #{booklet_num}: no anchor in DB')
        return True

    try:
        last_num = int(last_booklet['booklet_number'])
    except Exception:
        logger.warning(
            f"Unable to parse last_booklet['booklet_number']="
            f"{last_booklet.get('booklet_number')} as int; treating as no last_booklet"
        )
        return True

    threshold = last_num - lookback
    if booklet_num >= threshold:
        logger.debug(f'  insert {label} #{booklet_num}: >= threshold {threshold} (anchor={last_num}, lookback={lookback})')
        return True
    else:
        logger.debug(f'  skip {label} #{booklet_num}: {booklet_num} < threshold {threshold} (anchor={last_num}, lookback={lookback})')
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--last-law', type=int)
    parser.add_argument('-t', '--last-takana', type=int)
    parser.add_argument('-n', '--last-notification', type=int)
    parser.add_argument(
        '--lookback', type=int, default=DEFAULT_LOOKBACK,
        help=(
            f'How many booklet numbers behind the last known entry to re-check '
            f'for gaps (default: {DEFAULT_LOOKBACK})'
        )
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview what would be inserted into the DB and sent to Jira, without doing either'
    )
    parser.add_argument('--log')
    args = parser.parse_args()

    log = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
    }

    if args.log:
        logging.basicConfig(level=log[args.log])

    laws_dict = get_html('laws', DEFAULT_FETCH_LIMIT)
    takanot_dict = get_html('takanot', DEFAULT_FETCH_LIMIT)
    notifications_dict = get_html('notifications', DEFAULT_FETCH_LIMIT)

    logger.debug(f'API returned: {len(laws_dict["Results"])} laws, '
                 f'{len(takanot_dict["Results"])} takanot, '
                 f'{len(notifications_dict["Results"])} notifications')

    laws = list(clean_data(laws_dict, 'law'))
    takanot = list(clean_data(takanot_dict, 'takana'))
    notifications = list(clean_data(notifications_dict, 'notification'))

    logger.debug(f'after clean_data: {len(laws)} law entries, '
                 f'{len(takanot)} takana entries, '
                 f'{len(notifications)} notification entries')

    # Deduplicate within each batch by booklet_number, keeping the last occurrence
    def dedup(items):
        seen = {}
        for item in items:
            seen[item['booklet_number']] = item
        return list(seen.values())

    laws = dedup(laws)
    takanot = dedup(takanot)
    notifications = dedup(notifications)

    logger.debug(f'after dedup: {len(laws)} laws, {len(takanot)} takanot, {len(notifications)} notifications')

    with database.Database() as db:

        if args.last_law:
            last_law = db.get_law(args.last_law)
        else:
            last_law = db.get_last_law()

        if args.last_takana:
            last_takana = db.get_takana(args.last_takana)
        else:
            last_takana = db.get_last_takana()

        if args.last_notification:
            last_notification = db.get_notification(args.last_notification)
        else:
            last_notification = db.get_last_notification()

        existing_law_numbers = db.get_all_law_numbers()
        existing_takana_numbers = db.get_all_takana_numbers()
        existing_notification_numbers = db.get_all_notification_numbers()

        logger.debug(
            f'anchor laws: booklet #{last_law["booklet_number"] if last_law else "none"} '
            f'(lookback={args.lookback}, threshold='
            f'{int(last_law["booklet_number"]) - args.lookback if last_law else "n/a"})'
        )
        logger.debug(
            f'anchor takanot: booklet #{last_takana["booklet_number"] if last_takana else "none"}'
        )
        logger.debug(
            f'anchor notifications: booklet #{last_notification["booklet_number"] if last_notification else "none"}'
        )
        logger.debug(
            f'existing in DB: {len(existing_law_numbers)} law numbers, '
            f'{len(existing_takana_numbers)} takana numbers, '
            f'{len(existing_notification_numbers)} notification numbers'
        )

        laws = [law for law in laws
                if should_insert_booklet(last_law, law, existing_law_numbers, args.lookback)]
        takanot = [takana for takana in takanot
                   if should_insert_booklet(last_takana, takana, existing_takana_numbers, args.lookback)]
        notifications = [notification for notification in notifications
                         if should_insert_booklet(last_notification, notification, existing_notification_numbers, args.lookback)]

        if laws:
            logger.info(f'there are {len(laws)} new laws')
        else:
            logger.info(f'there are no new laws')

        if takanot:
            logger.info(f'there are {len(takanot)} new takanot')
        else:
            logger.info(f'there are no new takanot')

        if notifications:
            logger.info(f'there are {len(notifications)} new notifications')
        else:
            logger.info(f'there are no new notifications')

        for law in laws:
            if args.dry_run:
                print(f'[DRY RUN] would insert law: {law["booklet_number"]} – {law["display_name"]}')
            else:
                db.insert_law(law)

        for takana in takanot:
            if args.dry_run:
                print(f'[DRY RUN] would insert takana: {takana["booklet_number"]} – {takana["display_name"]}')
            else:
                db.insert_takana(takana)

        for notification in notifications:
            if args.dry_run:
                print(f'[DRY RUN] would insert notification: {notification["booklet_number"]} – {notification["display_name"]}')
            else:
                db.insert_notification(notification)

        all_items = list(laws)
        all_items.extend(takanot)
        all_items.extend(notifications)
        if all_items:
            if args.dry_run:
                print(f'[DRY RUN] would send {len(all_items)} item(s) to Jira')
            else:
                jira_api = JiraApi()
                jira_api.send(all_items)

    logger.info('done')


if __name__ == '__main__':
    main()
