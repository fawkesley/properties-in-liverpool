# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want:
# https://morph.io/documentation/python All that matters is that your final
# data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a
# table called "data".

import sys
import datetime
import logging
import os

import requests
import requests_cache
import dataset

from zoopla import Zoopla
from os.path import abspath, dirname, join as pjoin

API_KEY = os.environ['MORPH_ZOOPLA_API_KEY']
DATABASE_FILENAME = abspath(pjoin(dirname(__file__), 'data.sqlite'))
PAGE_SIZE = 100

GOOGLE_FORM_URL = os.environ['MORPH_GOOGLE_FORM_URL']
FORM_FIELDS = {
    'address': 'entry.549102214',
    'price': 'entry.1129300714',
    'bedrooms': 'entry.1139168906',
    'url': 'entry.1963784273',
    'map_url': 'entry.208918565'
}

MAP_URL = ('http://www.openstreetmap.org/'
           '?mlat={latitude}&mlon={longitude}&zoom=15')


def main(argv):
    logging.basicConfig(level=logging.INFO)

    zoopla = Zoopla(api_key=API_KEY)

    # db = dataset.connect('sqlite://{}'.format(DATABASE_FILENAME))
    db = dataset.connect('sqlite:///data.sqlite')
    table = db['data']

    for listing in get_listings(zoopla):
        print('£{:.0f}K {}/{}/{} — {} — {}'.format(
            listing.price / 1000,
            listing.num_bedrooms,
            listing.num_bathrooms,
            listing.num_recepts,
            listing.displayable_address,
            listing.details_url)
        )

        table.upsert(listing, ['listing_id'])

        posted_to_form = table.find_one(
            listing_id=listing.listing_id
        ).get('posted_to_form', False)

        if not posted_to_form:
            post_to_google_form(listing)
            table.upsert({
                'listing_id': listing.listing_id,
                'posted_to_form': True
                },
                ['listing_id']
            )


def post_to_google_form(listing):
    requests.post(
        GOOGLE_FORM_URL,
        data={
            FORM_FIELDS['address']: listing.displayable_address,
            FORM_FIELDS['price']: listing.price,
            FORM_FIELDS['bedrooms']: listing.num_bedrooms,
            FORM_FIELDS['url']: listing.details_url,
            FORM_FIELDS['map_url']: listing.map_url,
        }
    )


def get_listings(zoopla):

    for page_number in range(1, 11):  # max 10 x PAGE_SIZE = 1000

        with requests_cache.enabled():
            # https://developer.zoopla.co.uk/docs/read/Property_listings
            search = zoopla.property_listings({
                'minimum_price': 70000,
                'maximum_price': 130000,
                'minimum_beds': 2,
                # 'maximum_beds': 2,
                'listing_status': 'sale',
                'area': 'Liverpool City Centre',
                'include_sold': '0',
                'new_homes': 'false',
                'summarised': 'no',
                'page_number': page_number,
                'page_size': PAGE_SIZE,
                'order_by': 'age',
                'ordering': 'descending',
            })

        listings = search.pop('listing')

        for listing in listings:
            listing.details_url = listing.details_url.split('?')[0]
            listing.map_url = MAP_URL.format(
                        latitude=listing.latitude, longitude=listing.longitude
            )

            if should_filter(listing):
                logging.debug('Dropping {}'.format(listing.details_url))
                continue
            else:
                yield listing

        if len(listings) < PAGE_SIZE:
            break


def should_filter(listing):
    def agent_is_rw_invest_london(listing):
        return listing.agent_name == 'RW Invest London'

    filters = [
        agent_is_rw_invest_london
    ]

    return any([filt(listing) for filt in filters])


def to_datetime(date_string):
    """
    >>> to_datetime('2013-09-10 01:47:46')
    datetime(2013, 9, 10, 1, 47, 46)
    """
    return datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    main(sys.argv)
