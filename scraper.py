# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

import sys
import datetime
import logging
import os

import requests_cache
import dataset

from collections import OrderedDict
from pprint import pprint

from zoopla import Zoopla
from os.path import abspath, dirname, join as pjoin

API_KEY = os.environ['MORPH_ZOOPLA_API_KEY']
DATABASE_FILENAME = abspath(pjoin(dirname(__file__), 'data.sqlite'))
PAGE_SIZE = 100


def main(argv):
    logging.basicConfig(level=logging.INFO)

    zoopla = Zoopla(api_key=API_KEY)

    # db = dataset.connect('sqlite://{}'.format(DATABASE_FILENAME))
    db = dataset.connect('sqlite:///data.sqlite')
    table = db['data']

    for listing in get_listings(zoopla):
        #pprint(listing)
        pprint(listing.details_url)
        table.upsert(listing, ['listing_id'])

        # print(result.price)
        # print(result.description)
        # print(result.image_url)


def get_listings(zoopla):

    for page_number in range(1, 11):  # max 10 x PAGE_SIZE = 1000

        with requests_cache.enabled():
            # https://developer.zoopla.co.uk/docs/read/Property_listings
            search = zoopla.property_listings({
                'minimum_price': 70000,
                'maximum_price': 130000,
                'maximum_beds': 2,
                'listing_status': 'sale',
                'area': 'Liverpool City Centre',
                'summarised': 'no',
                'page_number': page_number,
                'page_size': PAGE_SIZE,
            })

        listings = search.pop('listing')

        for listing in listings:
            yield listing

        if len(listings) < PAGE_SIZE:
            break


class ExcludeListing(Exception):
    pass


def _get_listings(api):
    try:
        for listing in api.property_listings(
                area='Liverpool',
                output_type='area',
                listing_status='rent',
                include_rented='1',
                summarised=False,
                max_results=None):
            yield listing
    except RuntimeError as e:
        logging.exception(e)
        return


def make_row_from_listing(listing):
    logging.debug(listing.__dict__.keys())
    row = OrderedDict([
        ('listing_id', int(listing.listing_id)),
        ('url', listing.details_url),
        ('first_published_date', to_datetime(listing.first_published_date)),
        ('last_published_date', to_datetime(listing.last_published_date)),
        ('date_rented', None),
        ('outcode', listing.outcode),
        ('street_name', listing.street_name),
        ('property_type', listing.property_type),
        ('number_of_bedrooms', int(listing.num_bedrooms)),
        ('list_price', listing.price),
        ('status', listing.status),
        ('shared_ownership', is_shared_ownership(listing)),
        ('latitude', listing.latitude),
        ('longitude', listing.longitude),
        ('short_description', listing.short_description),
        ('agent_name', listing.agent_name)
    ])
    return row


def to_datetime(date_string):
    """
    >>> to_datetime('2013-09-10 01:47:46')
    datetime(2013, 9, 10, 1, 47, 46)
    """
    return datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


def is_shared_ownership(listing):
    try:
        modifier = listing.price_modifier
    except AttributeError:
        logging.debug("No price_modifier, assuming not shared ownership")
        return False  # If not specified, assume it's *not* shared ownership

    if modifier not in ('offers_over', 'fixed_price', 'offers_in_region_of',
                        'shared_equity', 'shared_ownership', 'poa', 'from',
                        'part_buy_part_rent', 'price_on_request',
                        'guide_price', 'sale_by_tender'):
        raise RuntimeError(
            "Unexpected price_modifier for listing #{}: '{}'".format(
                listing.listing_id, modifier))

    if modifier in ('poa', 'from', 'part_buy_part_rent', 'price_on_request',
                    'sale_by_tender'):
        raise ExcludeListing("Excluding #{}, price modifier='{}'".format(
            listing.listing_id, modifier))

    return listing.price_modifier.startswith('shared_')  # equity / ownership


if __name__ == '__main__':
    main(sys.argv)
