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


def should_insert_booklet(last_booklet, booklet, existing_entries: set, lookback: int):
    """
    Return True if this booklet should be inserted into the DB.

    Rules:
    - Skip if already in the DB (prevents duplicates). Uniqueness is determined
      by (booklet_number, display_name) so that different laws within the same
      booklet are each treated as distinct entries.
    - If no last_booklet exists, treat everything as new.
    - Otherwise accept anything within `lookback` items behind the last known
      number, so that gaps (items the API skipped on a previous run) are
      back-filled, as well as anything newer than the last known number.
    """
    booklet_num = int(booklet['booklet_number'])
    label = booklet.get('booklet_type', '?')

    if (booklet_num, booklet['display_name']) in existing_entries:
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
        '--resend', type=int, metavar='BOOKLET_NUMBER',
        help='Fetch this booklet from the DB and resend it to Jira without modifying the DB'
    )
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

    if args.resend:
        with database.Database() as db:
            items = db.get_full_by_booklet_number(args.resend)
            if not items:
                logger.error(f'booklet #{args.resend} not found in DB')
                return
            logger.info(f'resending booklet #{args.resend} to Jira ({len(items)} row(s))')
            jira_api = JiraApi()
            sent = jira_api.send(items, dry_run=args.dry_run)
            for datum, jira_key in sent:
                db.update_jira_key_by_id(datum['id'], jira_key)
        return

    laws_dict = get_html('laws', DEFAULT_FETCH_LIMIT)
    takanot_dict = get_html('takanot', DEFAULT_FETCH_LIMIT)
    notifications_dict = get_html('notifications', DEFAULT_FETCH_LIMIT)

    logger.debug(f'API returned: {len(laws_dict["Results"])} laws, '
                 f'{len(takanot_dict["Results"])} regulations, '
                 f'{len(notifications_dict["Results"])} notifications')

    laws = list(clean_data(laws_dict, 'law'))
    takanot = list(clean_data(takanot_dict, 'takana'))
    notifications = list(clean_data(notifications_dict, 'notification'))

    logger.debug(f'after clean_data: {len(laws)} law entries, '
                 f'{len(takanot)} regulation entries, '
                 f'{len(notifications)} notification entries')

    # Deduplicate within each batch: the API can return the same entry twice.
    # Key by (booklet_number, display_name) so different laws within the same
    # booklet are kept as separate entries.
    def dedup(items):
        seen = {}
        for item in items:
            seen[(item['booklet_number'], item['display_name'])] = item
        return list(seen.values())

    laws = dedup(laws)
    takanot = dedup(takanot)
    notifications = dedup(notifications)

    logger.debug(f'after dedup: {len(laws)} laws, {len(takanot)} regulations, {len(notifications)} notifications')

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

        existing_law_entries = db.get_all_law_entries()
        existing_takana_entries = db.get_all_takana_entries()
        existing_notification_entries = db.get_all_notification_entries()

        logger.debug(
            f'anchor laws: booklet #{last_law["booklet_number"] if last_law else "none"} '
            f'(lookback={args.lookback}, threshold='
            f'{int(last_law["booklet_number"]) - args.lookback if last_law else "n/a"})'
        )
        logger.debug(
            f'anchor regulations: booklet #{last_takana["booklet_number"] if last_takana else "none"}'
        )
        logger.debug(
            f'anchor notifications: booklet #{last_notification["booklet_number"] if last_notification else "none"}'
        )
        logger.debug(
            f'existing in DB: {len(existing_law_entries)} law entries, '
            f'{len(existing_takana_entries)} regulation entries, '
            f'{len(existing_notification_entries)} notification entries'
        )

        laws = [law for law in laws
                if should_insert_booklet(last_law, law, existing_law_entries, args.lookback)]
        takanot = [takana for takana in takanot
                   if should_insert_booklet(last_takana, takana, existing_takana_entries, args.lookback)]
        notifications = [notification for notification in notifications
                         if should_insert_booklet(last_notification, notification, existing_notification_entries, args.lookback)]

        def _summary_line(label, items):
            if items:
                numbers = ', '.join(str(i['booklet_number']) for i in items)
                return f'  {len(items)} new {label}: {numbers}'
            return f'  0 new {label}'

        logger.info('Retrieved:\n' + '\n'.join([
            _summary_line('laws', laws),
            _summary_line('regulations', takanot),
            _summary_line('notifications', notifications),
        ]))

        for law in laws:
            if args.dry_run:
                print(f'[DRY RUN] would insert law: {law["booklet_number"]} – {law["display_name"]}')
            else:
                law['id'] = db.insert_law(law)

        for takana in takanot:
            if args.dry_run:
                print(f'[DRY RUN] would insert takana: {takana["booklet_number"]} – {takana["display_name"]}')
            else:
                takana['id'] = db.insert_takana(takana)

        for notification in notifications:
            if args.dry_run:
                print(f'[DRY RUN] would insert notification: {notification["booklet_number"]} – {notification["display_name"]}')
            else:
                notification['id'] = db.insert_notification(notification)

        all_items = list(laws)
        all_items.extend(takanot)
        all_items.extend(notifications)
        if all_items:
            logger.info(f'Sending {len(all_items)} item(s) to Jira')
            if args.dry_run:
                print(f'[DRY RUN] would send {len(all_items)} item(s) to Jira')
            else:
                jira_api = JiraApi()
                sent = jira_api.send(all_items)
                for datum, jira_key in sent:
                    db.update_jira_key_by_id(datum['id'], jira_key)

    logger.info('done')


if __name__ == '__main__':
    main()
