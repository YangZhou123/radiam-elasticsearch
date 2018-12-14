#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""diskover - Elasticsearch file system crawler
diskover is a file system crawler that index's
your file metadata into Elasticsearch.
See README.md or https://github.com/shirosaidev/diskover
for more information.

Copyright (C) Chris Park 2017-2018
diskover is released under the Apache 2.0 license. See
LICENSE for the full license text.
"""

try:
    from elasticsearch5 import Elasticsearch, helpers, RequestsHttpConnection, \
        Urllib3HttpConnection, exceptions
except ImportError:
    try:
        from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection, \
            Urllib3HttpConnection, exceptions
    except ImportError:
        raise ImportError('elasticsearch module not installed')

es_conn = None

def connect_to_elasticsearch():
    from radiam import config
    global es_conn
    # Local connection to es
    es_conn = Elasticsearch(
        hosts=[{'host': config['es_host'], 'port': config['es_port']}],
        http_auth=(config['es_user'], config['es_password']),
        connection_class=Urllib3HttpConnection,
        timeout=config['es_timeout'], maxsize=config['es_maxsize'],
        max_retries=config['es_max_retries'], retry_on_timeout=True)

