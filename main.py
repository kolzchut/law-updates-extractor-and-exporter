#!/usr/bin/env python

import argparse
import logging
from pprint import pprint

from air_table_api import AirTable
from cleaner import clean_data
import database
from scraper import get_html
from jira import JiraApi


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def should_insert_booklet(last_booklet, booklet):
    return True # not last_booklet or last_booklet['booklet_number'] < int(booklet['booklet_number'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date')
    parser.add_argument('-l', '--last-booklet', type=int)
    args = parser.parse_args()

    laws_dict = get_html('laws', 100)
    takanot_dict = get_html('takanot', 100)

    laws = clean_data(laws_dict, 'law')
    takanot = clean_data(takanot_dict, 'takana')

    with database.Database() as db:

        last_law = db.get_last_law()
        if args.last_booklet:
            last_takana = db.get_takana(args.last_booklet)
        else:
            last_takana = db.get_last_takana()

        laws = [law for law in laws if should_insert_booklet(last_law, law)]
        takanot = [takana for takana in takanot if should_insert_booklet(last_takana, takana)]

        air_table = AirTable()

        for law in laws:
            db.insert_law(law)

        for takana in takanot:
            db.insert_takana(takana)

        laws = list(laws)
        laws.extend(takanot)
        if laws:
            air_table.create_records(laws)
            jira_api = JiraApi()
            jira_api.send(laws)

    logger.info('done')


if __name__ == '__main__':
    main()
