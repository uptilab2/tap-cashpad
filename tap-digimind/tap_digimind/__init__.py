#!/usr/bin/env python3
import argparse
from datetime import date, timedelta, datetime
import json
import os
import re
import shlex

import requests
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = ["start_date", "api_export_command"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=[],
                metadata=stream_metadata,
                replication_key='date',
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method="FULL_TABLE",
            )
        )
    return Catalog(streams)


def parse_api_export_command(command):
    """ Parse config export command to extract all mandatory information
    needed to query api: Client id, authentication, accounts....
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('prog')
    parser.add_argument('--user')
    parser.add_argument('--data')
    args, unparsed_args = parser.parse_known_args(shlex.split(command))

    # Check we have a curl command
    assert args.prog == 'curl'

    # Extract user / password from user argument
    auth = tuple(args.user.split(':', 1))
    if auth[0] == 'MYLOGIN':
        raise Exception('Please replace MYLOGIN:MYPASSWORD with actual credentials')

    # Extract client_id from api url
    url_match = re.match('https://social.digimind.com/d/([a-zA-Z0-9]+)/api/measurement/publications/top', unparsed_args[-1])
    client_id = url_match.group(1)
    api_url = f'https://social.digimind.com/d/{client_id}/api'

    # Extract topic
    data = json.loads(args.data)
    topic_id = data['topic']
    accounts = data['accounts']

    return api_url, auth, topic_id, accounts


def get_interactions_data(api_url, auth, topic_id, accounts, start_date, end_date):

    rows = []
    for account in accounts:

        LOGGER.info(f'Fetching daily interactions for actor {account["webActorType"]} {account["actorId"]} from {start_date} to {end_date}')
        post_data = {
            "topic": topic_id,
            "accounts": [account],
            "fromDate": start_date + 'T00:00:00.000Z',
            "toDate": end_date + 'T23:59:59.999Z',
        }
        response = requests.post(api_url + '/measurement/interactions/dailyInteractions', json=post_data, auth=auth)

        if response.status_code != 200:
            LOGGER.error(post_data)
            LOGGER.error(response.text)
            response.raise_for_status()

        response_data = response.json()

        raw_interactions = [f"{account['webActorType'].lower()}_{serie['title']}" for serie in response_data['series']]
        raw_values = [serie['values'] for serie in response_data['series']]
        values = []
        interactions = []
        for i, value in enumerate(raw_values):
            if len(value) != 0:
                values.append(value)
                interactions.append(raw_interactions[i])

        for records in zip(*values):
            # Iterate all series on date
            row = {
                'topic_id': str(topic_id),
                'network': account['webActorType'],
                'actor_id': account['actorId'],
                'source_id': str(account['sourceId']),
            }

            for record, interaction_name in zip(records, interactions):
                row.setdefault('date', record['time'][:10])
                row[interaction_name] = int(record['count'])
            rows.append(row)

    return rows


def get_subscriptions_data(api_url, auth, topic_id, accounts, start_date, end_date):
    """ Returns the daily subscribers total for each account """
    rows = []

    for account in accounts:
        LOGGER.info(f'Fetching daily totalSubscribers for actor {account["webActorType"]} {account["actorId"]} from {start_date} to {end_date}')
        post_data = {
            "topic": str(topic_id),
            "accounts": [account],
            "fromDate": start_date + 'T00:00:00.000Z',
            "toDate": end_date + 'T23:59:59.999Z',
        }

        response = requests.post(api_url + '/measurement/community/totalSubscribers', json=post_data, auth=auth)

        if response.status_code != 200:
            LOGGER.error(post_data)
            LOGGER.error(response.text)
            response.raise_for_status()

        response_data = response.json()
        for record in response_data['series']:
            for values in record['values']:
                rows.append({
                    'date': values['time'][:10],
                    'topic_id': str(topic_id),
                    'network': account['webActorType'],
                    'actor_id': account['actorId'],
                    'source_id': str(account['sourceId']),
                    'subscriptions': int(values['count']),
                    })

    return rows


def get_top_publications_data(api_url, auth, topic_id, accounts, start_date, end_date):
    """ Returns the daily subscribers total for each account """
    rows = []
    for account in accounts:
        LOGGER.info(f'Fetching daily top publications for actor {account["webActorType"]} {account["actorId"]} from {start_date} to {end_date}')

        post_data = {
                "topic": str(topic_id),
                "accounts": [account],
                "fromDate": start_date + 'T00:00:00.000Z',  # single date here
                "toDate": end_date + 'T23:59:59.999Z',
                "page": 1,
                }
        response = requests.post(api_url + '/measurement/publications/top', json=post_data, auth=auth)

        if response.status_code != 200:
            LOGGER.error(post_data)
            LOGGER.error(response.text)
            response.raise_for_status()

        response_data = response.json()

        if response_data['count']:
            count = -1
            while(count <= response_data['count']/10):
                post_data['page'] = count
                count += 1
                response = requests.post(api_url + '/measurement/publications/top', json=post_data, auth=auth)

                if response.status_code != 200:
                    LOGGER.error(post_data)
                    LOGGER.error(response.text)
                    response.raise_for_status()

                response_data = response.json()

                undef = 0
                for record in response_data['publication']:

                    if record['mediaType'] == "UNDEFINED":
                        undef += 1
                        continue
                    rows.append({
                        'date': end_date,
                        'topic_id': str(topic_id),
                        'network': account['webActorType'],
                        'actor_id': account['actorId'],
                        'source_id': str(account['sourceId']),
                        'id': record['id'],
                        'title': record['title'],
                        'author': record['author'],
                        'description': record['description'],
                        'publication_date': datetime.fromtimestamp(record['date'] // 1000).isoformat(),
                        'url': record['url'],
                        'image': record['image'],
                        'media_type': record['mediaType'],
                        'facebook_shares': record['fbShares'],
                        'facebook_likes': record['fbLikes'],
                        'facebook_comments': record['fbComments'],
                        'instagram_likes': record['igLikes'],
                        'replies': record['replies'],
                        'youtube_likes': record['vdLikes'],
                        'youtube_views': record['vdViews'],
                        'linkedin_likes': record['inLikes'],
                        'linkedin_comments': record['inComments'],
                        'twitter_favorited': record['twFavorited'],
                        'twitter_retweet': record['twRetweet'],
                        'twitter_replies': record['twReplies'],
                        'total_interactions': record['totalInteractions'],
                        'pinterest_likes': record['ptLikes'],
                        'pinterest_repins': record['ptRepins'],
                        'pinterest_comments': record['ptComments'],
                        'comments': record['comments'],
                        'backlinks': record['backlinks'],
                        'max_interactions': record['maxInteractions'],
                    })

    return rows


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Parse connection credentials and settings from config
    api_url, auth, topic_id, accounts = parse_api_export_command(config['api_export_command'])

    # Get end date from current date
    end_date = (date.today() - timedelta(days=1)).isoformat()

    # Get default start_date from config (used when no state exists)
    start_date = config['start_date']

    # Loop over selected streams in catalog
    for stream in catalog.streams:
        if not stream.schema.selected:
            continue
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=[],
        )

        if start_date > end_date:
            LOGGER.info(f'Skipping stream "{stream.tap_stream_id}": no date interval to sync')
            continue

        if stream.tap_stream_id == 'interactions':
            tap_data = get_interactions_data(api_url, auth, topic_id, accounts, start_date, end_date)
        elif stream.tap_stream_id == 'subscriptions':
            tap_data = get_subscriptions_data(api_url, auth, topic_id, accounts, start_date, end_date)
        elif stream.tap_stream_id == 'top_publications':
            tap_data = get_top_publications_data(api_url, auth, topic_id, accounts, start_date, end_date)
        else:
            raise Exception(f'Stream not yet implemented: {stream.tap_stream_id}')

        for row in tap_data:
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])

        # update bookmark to latest value
        state[stream.tap_stream_id] = end_date

    #singer.write_state(state)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if not args.catalog:
            raise Exception('missing catalog')
        sync(args.config, args.state, args.catalog)


if __name__ == "__main__":
    main()
