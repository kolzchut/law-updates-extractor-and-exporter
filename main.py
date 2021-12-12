#!/usr/bin/env python

import argparse
import logging

from cleaner import clean_data
import database
from scraper import get_html
from jira import JiraApi


logger = logging.getLogger(__name__)


def should_insert_booklet(last_booklet, booklet):
    return not last_booklet or last_booklet['booklet_number'] < int(booklet['booklet_number'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--last-law', type=int)
    parser.add_argument('-t', '--last-takana', type=int)
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

    laws_dict = get_html('laws', 100)
    takanot_dict = get_html('takanot', 100)

    laws = clean_data(laws_dict, 'law')
    takanot = clean_data(takanot_dict, 'takana')

    with database.Database() as db:

        if args.last_law:
            last_law = db.get_law(args.last_law)
        else:
            last_law = db.get_last_law()

        if args.last_booklet:
            last_takana = db.get_takana(args.last_takana)
        else:
            last_takana = db.get_last_takana()

        laws = [law for law in laws if should_insert_booklet(last_law, law)]
        takanot = [takana for takana in takanot if should_insert_booklet(last_takana, takana)]

        if laws:
            logger.info(f'there are {len(laws)} new laws')
        else:
            logger.info(f'there are no new laws')

        if takanot:
            logger.info(f'there are {len(takanot)} new takanot')
        else:
            logger.info(f'there are no new takanot')

        for law in laws:
            db.insert_law(law)

        for takana in takanot:
            db.insert_takana(takana)

        laws = list(laws)
        laws.extend(takanot)
        if laws:
            jira_api = JiraApi()
            jira_api.send(laws)

    logger.info('done')


if __name__ == '__main__':
    main()
