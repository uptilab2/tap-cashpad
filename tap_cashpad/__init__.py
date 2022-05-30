#!/usr/bin/env python3
# import os
# import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

import requests
from requests.adapters import HTTPAdapter, Retry

from typing import List, Dict
from datetime import datetime

from .schemas import ARCHIVE_CONTENT, PRODUCTS_SUMMARY, LIVE_DATA, SALES_SUMMARY


REQUIRED_CONFIG_KEYS = ["installation_id", "apiuser_email", "apiuser_token"]
VERSION = "v2"
BASE_URL = f"https://www6.cashpad.net/api/salesdata/{VERSION}/"
LOGGER = singer.get_logger()

# This will allow to query with requests_retries which include exponetial backoff retry instead of requests
requests_session = requests.Session()
http_retry_codes = (500, 502, 503, 504, 506, 507, 520, 521, 522, 523, 524, 525, 527, 429, 444, 498, 499)
requests_retries = Retry(total=5, backoff_factor=10, status_forcelist=http_retry_codes)
requests_session.mount('', HTTPAdapter(max_retries=requests_retries))


# class Node:
#     """Node represents the keys of a schema and link a key to its parent. It is useful to get a list of parents for
#     a given key"""

#     def __init__(self, key: str, parent=None):
#         self.parent = parent
#         if parent:
#             parent.set_child(self)
#         self.child = None
#         self.key = key

#     def set_child(self, child):
#         """this method is to change a parent's child"""
#         self.child = child

#     def key_nodes_list(self, excluded_keys={}):
#         """This method give for a node all the genealogy of its familly sorted from the eldest parents to the node
#         itself """
#         list = []
#         parent = self.parent

#         list = [self.key] if self.key not in excluded_keys else list

#         have_parent = True if self.parent else False
#         while have_parent:
#             if isinstance(parent, Node):
#                 if parent.key not in excluded_keys:
#                     list.append(parent.key)
#                 parent = parent.parent
#             else:
#                 have_parent = False

#         return list[::-1]


# def data_validator(schema, key_list):
#     """Compare a list of keys tracing the parents of an element to a schema.
#     It works by checking each key exist in the schema. And each path exist in the schema"""

#     for key in key_list:
#         if key in schema.keys():
#             schema = schema.get(key)
#         else:
#             return "faut supprimer"
#     return "ok"


# def dict_parser(input, parent_node=[], node_list=[]):
#     """Parse a dict to extract for each key it's parents and instanciate a node for each keys linked to its parents"""
#     key_list = input.keys()
#     new_node = None

#     for key in key_list:
#         if key in key_list:
#             if isinstance(input, dict):
#                 new_input = input.get(key)
#                 new_node = Node(key, parent_node)
#                 node_list.append(new_node)

#             input_strategy(new_input, parent_node=new_node, node_list=node_list)
#     return node_list


# def list_parser(input, parent_node, node_list):
#     """Parse a list in order to check for each elements the keys it contains"""
#     for element in input:
#         input_strategy(element, parent_node, node_list=node_list)

#     return


# def input_strategy(input, parent_node=None, trace=[], node_list=[]):
#     """Decide which parsing strategy we should follow checking whether the input is a list or a dict"""
#     if isinstance(input, dict):
#         trace = dict_parser(input, parent_node, node_list=node_list)

#     elif isinstance(input, list):
#         list_parser(input, parent_node, node_list=node_list)

#     return trace

# # TODO implement this in code to check response.json from api is valid, for each unexpected row we delete the key from the response before singer.write
# schema = {}
# data = {}
# verified = input_strategy(schema)
# final_traces = []

# excluded_keys = {"type", "items", "properties"}

# for verif in verified:
#     if verif.key not in excluded_keys:
#         final_traces.append(verif.key_nodes_list(excluded_keys))

# # print("final_trace is: ", final_traces)

# for trace in final_traces:
#     print(data_validator(data, trace))


# def get_abs_path(path: str) -> str:
#     return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> Dict:
    """ Load schemas from schemas folder """
    schemas = {}
    schema_available = {
        "archive_content": ARCHIVE_CONTENT,
        "products_summary": PRODUCTS_SUMMARY,
        "live_data": LIVE_DATA,
        "sales_summary": SALES_SUMMARY,
    }
    for table_name, schema in schema_available.items():
        schemas[table_name] = Schema.from_dict(schema)
    return schemas


def discover() -> object:
    """ This discover schemas from schemas folder and generates streams from them """
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key="sequential_id" if stream_id != "live_data" else None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method="INCREMENTAL" if stream_id != "live_data" else 'FULL_TABLE',
            )
        )
    return Catalog(streams)


def get_closure_list(config: Dict, start_sequential_id: int = None) -> List:
    """ This send a list of closure we have to retrieve. It uses start_sequential_id and add 1 to it to get only new
    available closure from Cashpad. Indeed Cashpad start from the start_sequential_id given and will send again data we
    already wrote.
    Example: If start_sequential_id = 1 cashpad will send closure 1, 2, 3 ... Where we just want closure 2, 3 ... """

    archive_url = BASE_URL + config.get("installation_id") + "/archives"
    params = {
        "apiuser_email": config.get("apiuser_email"),
        "apiuser_token": config.get("apiuser_token"),
    }
    if start_sequential_id:
        params["start_sequential_id"] = start_sequential_id + 1  # Add 1 to get next id

    closure_list = []
    LOGGER.info(f"[archives] requesting data")

    response = requests_session.get(archive_url, params=params)
    LOGGER.info(f"[archives] response status code is: {response.status_code}")

    if response.status_code == 200:
        for row in response.json().get("data"):
            closure_list.append(
                {
                    "sequential_id": row.get("sequential_id"),
                    "range_begin_date": row.get("range_begin_date"),
                    "range_end_date": row.get("range_end_date")
                }
            )
    return closure_list


def get_live_closing(config: Dict, sequential_id: int = None, version: int = None) -> Dict:
    """ Get content for not closed data. Data sent as output as not fully consolidated and can change in a later call
    We will add missing items to match archive_content.json schema and is_closed flag to false to mark these data are
    live """

    archive_url = BASE_URL + config.get("installation_id") + "/live_data"
    params = {
        "apiuser_email": config.get("apiuser_email"),
        "apiuser_token": config.get("apiuser_token"),
    }

    if sequential_id:
        params["sequential_id"] = sequential_id
    if version:
        params["version"] = version

    LOGGER.info(f"[live_data] requesting data")

    response = requests_session.get(archive_url, params=params)
    LOGGER.info(f"[live_data] response status code is: {response.status_code}")

    if response.status_code == 200:
        live_data = response.json().get("data")

        # Case where no new live data
        if not live_data:
            LOGGER.info("No new live data available")
            return []

        # Mark data are ongoing
        live_data["is_closed"] = False

        return [live_data]


def get_closed(config: Dict, closure_list: List) -> List:
    """ Get archive content for a closure list """
    archive_content = BASE_URL + config.get("installation_id") + "/archive_content"
    content_list = []
    for closed in closure_list:
        sequential_id = closed.get("sequential_id")
        params = {
            "sequential_id": sequential_id,
            "apiuser_email": config.get("apiuser_email"),
            "apiuser_token": config.get("apiuser_token"),
        }
        LOGGER.info(f"[archive_content] requesting data for sequential_id: {sequential_id}")

        response = requests_session.get(archive_content, params=params)

        LOGGER.info(f"[archive_content] response status code is: {response.status_code}")

        if response.status_code == 200:
            content_list.append(response.json().get("data"))
        elif response.status_code == 400:
            LOGGER.error(f"The sequential_id: {closed.get('sequential_id')} requested doesn't exist")
            continue
    return content_list


def get_product_summary(config: Dict, closure_list: List) -> List:
    """ Get product_summary """
    products_summary = BASE_URL + config.get("installation_id") + "/products_summary"
    content_list = []
    for closed in closure_list:
        sequential_id = closed.get("sequential_id")
        params = {
            "sequential_id": sequential_id,
            "apiuser_email": config.get("apiuser_email"),
            "apiuser_token": config.get("apiuser_token"),
        }
        LOGGER.info(f"[products_summary] requesting data for sequential_id: {sequential_id}")

        response = requests_session.get(products_summary, params=params)

        LOGGER.info(f"[products_summary] response status code is: {response.status_code}")

        if response.status_code == 200:
            content_list.append(response.json().get("data"))
        elif response.status_code == 400:
            LOGGER.error(f"The sequential_id: {closed.get('sequential_id')} requested doesn't exist")
            continue
    return content_list


def get_sales_summary(config: Dict, closure_list: List) -> List:
    """ Get product_summary """
    sales_summary = BASE_URL + config.get("installation_id") + "/sales_summary"
    content_list = []
    for closed in closure_list:
        sequential_id = closed.get("sequential_id")
        params = {
            "sequential_id": sequential_id,
            "apiuser_email": config.get("apiuser_email"),
            "apiuser_token": config.get("apiuser_token"),
        }
        LOGGER.info(f"[sales_summary] requesting data for sequential_id: {sequential_id}")

        response = requests_session.get(sales_summary, params=params)

        LOGGER.info(f"[sales_summary] response status code is: {response.status_code}")

        if response.status_code == 200:
            content_list.append(response.json().get("data"))
        elif response.status_code == 400:
            LOGGER.error(f"The sequential_id: {closed.get('sequential_id')} requested doesn't exist")
            continue
    return content_list


def sync(config: Dict, state: Dict, catalog: object) -> None:
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    batch_write_timestamp = datetime.now().__str__()

    new_state = state

    for stream in catalog.get_selected_streams(state):

        LOGGER.info(f"[{stream.tap_stream_id}] ******** Starting sync stream ********")
        LOGGER.info(f'[{stream.tap_stream_id}] info:')
        LOGGER.info(f'......replication method: {stream.replication_method}')
        LOGGER.info(f'......replication key: {stream.replication_key}')

        bookmark_column = stream.replication_key
        tap_data = []
        closure_list = []
        is_sorted = True
        start_sequential_id = None

        if stream.tap_stream_id != 'live_data':
            # Here we get list of closed data, that means the data retrived by this function are unmutable and can be
            # ingested by target safely
            if new_state:
                start_sequential_id = new_state.get(stream.tap_stream_id, None)
                LOGGER.info(f'[{stream.tap_stream_id}] state found')
                LOGGER.info(f'[{stream.tap_stream_id}] last sequential_id sync: {start_sequential_id}')
            closure_list = get_closure_list(config, start_sequential_id)
            LOGGER.info(f"[{stream.tap_stream_id}] sequential_ids to sync: {[c.get('sequential_id') for c in closure_list] if closure_list else 'None'}")
        # Here we get list of ongoing data, that means the data retrived by this function are mutable and can change by
        # the time you call Cashpad API. We mark these data as not closed and append them to the target write.
        # Data analyst should take care of them later thanks to the ingestion date in order to get only fresh data.
        # live_data = get_live_closing(config)
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        # Case where no more closed data
        if len(closure_list) == 0 and stream.tap_stream_id != 'live_data':
            LOGGER.info(f"[{stream.tap_stream_id}] No new archive available")
        else:
            data = None
            # TODO: maybe we should separate each object and have their own table
            # and refacto request method like products_sum, sales_sum, archive_content is similar
            if stream.tap_stream_id == 'products_summary':
                data = get_product_summary(config, closure_list)
            elif stream.tap_stream_id == 'sales_summary':
                data = get_sales_summary(config, closure_list)
            elif stream.tap_stream_id == 'archive_content':
                data = get_closed(config, closure_list)
            elif stream.tap_stream_id == 'live_data':
                data = get_live_closing(config)
            else:
                LOGGER.error("Stream id not recognized")
                raise Exception()
            # get_closed return a list of list so we just extend it in tap_data
            tap_data.extend(data)

        # live_data contain just one list we can append to tap_data
        # tap_data.append(live_data)

        # Case where no new data at all
        if len(tap_data) == 0:
            LOGGER.info(f"[{stream.tap_stream_id}] No new data available")

        max_bookmark = None
        for row in tap_data:
            row["ingestion_date"] = batch_write_timestamp
            row["is_closed"] = row.get("is_closed") if row.get("is_closed", None) is False else True
            # Write row to the stream for target :
            parsed_row = transform(row, stream.schema.to_dict())
            singer.write_records(stream.tap_stream_id, [parsed_row])

            if bookmark_column and row.get("is_closed"):
                if is_sorted:
                    # update bookmark to latest value
                    new_state = {**new_state, stream.tap_stream_id: row[bookmark_column]}
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted and row.get("is_closed"):
            new_state = {**new_state, stream.tap_stream_id: max_bookmark}

        singer.write_state(new_state)

        LOGGER.info(f"[{stream.tap_stream_id}] ******** Ending sync stream ********")
    return


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
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
