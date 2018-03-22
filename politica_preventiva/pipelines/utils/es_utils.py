#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" ES Utils
This module contains utilities for AWS-ElasticSearch Service
"""

import boto3
import datetime
import json
import requests
import urllib

from elasticsearch import Elasticsearch, RequestsHttpConnection
import pandas as pd
from pprint import pprint


def connect_es(esEndPoint):
    """Connect to ElasticSearch domain
      Args:
        esEndPoint (str): Domain Address of ES endpoint

  """
    print ('Connecting to the ES Endpoint {0}'.format(esEndPoint))
    try:
        esClient = Elasticsearch(
        hosts=[esEndPoint], #,'port': 443}],
        use_ssl=True,
            verify_certs=True,
        connection_class=RequestsHttpConnection)
        return esClient

    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)


def create_index(esClient, index_name, indexDoc, replace=True):
    """Check if index exists, if not create.
     Args:
        esClient (obj): Connection to ElasticSearch
        index_name (str): ES index name.
        indexDoc: Schema of document.
        replace (bool): Bool replace (True) or append (False)
   """

    try:
        existance = esClient.indices.exists(index_name)

        if existance is False:
            esClient.indices.create(index_name, body=indexDoc)

        elif existance is True and replace is True:
            # Remove content of index
            esClient.indices.delete(index=index_name,
            ignore=[400, 404])
            esClient.indices.create(index_name, body=indexDoc)

        else:
            # TODO() check -> ElasticSearch delete_by_query
            pass ##

    except Exception as E:
        print("Unable to Create Index {0}".format(index_name))
        print(E)


def remove_doc_element(esClient, index_name, doc_type, docId):
    """Function to remove doc from index if it exists.
    Args:
        esClient (obj): Connection to ElasticSearch
        index_name (str): ES index name
        doc_type (str): ES document type
        docId (str): Id of the document

   """

    try:
        retval = esClient.delete(index=index_name, doc_type=doc_type, id=docId)
        print("Deleted: " + docId)
        return 1

    except Exception as E:
        print("DocId delete command failed at Elasticsearch.")
        print("Error: ",E)


def index_pandas_to_es(esClient, index_name, doc_type, pandas_db):
    """Upload a pandas db into your index.
    Args:
        esClient (obj): Connection to ElasticSearch
        index_name (str): ES index name
        doc_type (str): ES doc type
        pandas_db (DataFrame): Pandas Dataframe

    """
    # TODO()
    # Replace de id-index to task date (y/month)
	# Define data date
    data_date = str(datetime.date.today())
    try:
        data_js = pandas_db.to_dict(orient='records')
        retval = esClient.index(index=index_name, doc_type=doc_type,
                                body=data_js, timestamp=data_date)
    except Exception as E:
        print("Document not indexed")
        print("Error: ",E)

def index_pandas_to_es_by_chucks(esEndPoint, index_name, doc_type,
                                 pandas_db, chunk_size = 2000):
    """Upload a doc into your index by chunks using requests
    Args:
        esEndPoint (str): ES Domain address.
        index_name (str): Index name in ES. (It should already exists)
        doc_type (str): type of document.
        pandas_db (str): Pandas DataFrame.
        chunk_size (int): Number of rows of db to upload at each iteration.

    TODO() Update this function to use elasticsearch library.
    """
    i=0
    headers = {'content-type': 'application/x-ndjson', 'Accept-Charset': 'UTF-8'}
    records = pandas_db.to_dict(orient='records')
    actions = ["""{ "index" : { "_index" : "%s", "_type" : "%s"} }\n""" % (index_name,
                                                                           doc_type) +\
    json.dumps(records[j]) for j in range(len(records))]

    while i<len(actions):
        # Ingest with Bulk
        esEndPointAPI = esEndPoint + '/_bulk'
        data='\n'.join(actions[i:min([i+chunk_size,len(actions)])])
        data = data + '\n'
        r = requests.post(esEndPointAPI, data = data, headers=headers)
        print(r.content)
        i = i + chunk_size

