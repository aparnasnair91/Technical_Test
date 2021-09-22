import json

from io import BytesIO

import requests

from bs4 import UnicodeDammit
from lxml.html import fromstring
from warcio import ArchiveIterator


def load_single_warc_record(source: str, offset: int, length: int) -> str:
    """Pull a single WARC record from S3:CommonCrawl"""
    record_end = offset + length - 1
    response = requests.get(source, headers={'Range': 'bytes=%s-%s' % (offset, record_end)})
    content = response.content

    gz_body = BytesIO()
    gz_body.write(content)
    gz_body.seek(0)

    warc_iterator = iter(ArchiveIterator(gz_body))
    final_warc_html = None
    for record in warc_iterator:
        WARC_target_URI = record.rec_headers.get_header('WARC-Target-URI')
        if record.rec_type == 'response':
            if WARC_target_URI.endswith('/source'):
                html_info = record.content_stream().read().decode()
                html_json = json.loads(html_info)
                # save it as a byte to conform to output type
                final_warc_html = html_json['value'].encode()
            elif record.rec_type == 'response':
                # Read the HTML content here, as you lose the buffer when the for loop advances
                final_warc_html = record.content_stream().read()
    html_str = UnicodeDammit(final_warc_html, is_html=True).unicode_markup
    return html_str


def get_html_extracts(html: str):
    # COMPLETED : New implementation to consolidate all tags
    nodes_iter = fromstring(html).iter()
    tags_extracted = []
    for node in nodes_iter:
        tag = node.tag
        left_siblings_raw = node.xpath('./preceding-sibling::*')
        left_siblings = left_siblings_raw[-1].tag if left_siblings_raw else None

        right_siblings_raw = node.xpath('./following-sibling::*')
        right_siblings = right_siblings_raw[0].tag if right_siblings_raw else None

        parent_raw = node.xpath('./parent::*')
        parent = parent_raw[0].tag if parent_raw else None

        extractor = [node, tag, left_siblings, right_siblings, parent]
        yield extractor


def get_nodes(html: str):
    doc_tree = fromstring(html)
    return doc_tree.iter()


def get_node_tags(node_iterator):
    for node in node_iterator:
        yield node.tag


def get_left_sibling_tags(node_iterator):
    for node in node_iterator:
        left_siblings = node.xpath('./preceding-sibling::*')
        yield left_siblings[-1].tag if left_siblings else None


def get_right_sibling_tags(node_iterator):
    for node in node_iterator:
        right_siblings = node.xpath('./following-sibling::*')
        yield right_siblings[0].tag if right_siblings else None
        # yield right_siblings if right_siblings else None


def get_parent_tags(node_iterator):
    # COMPLETED:
    for node in node_iterator:
        parent = node.xpath('./parent::*')
        yield parent[0].tag if parent else None
