"""Take a word, execute query against ES, show nearest words"""

from elasticsearch import Elasticsearch, helpers
import pdb
import sys

if __name__ == '__main__':

    assert len(sys.argv) > 1
    index = sys.argv[1]
    doc_type = sys.argv[2]
    word = sys.argv[3]

    es = Elasticsearch()
    res = es.search(
        index=index, doc_type=doc_type,
        body={"query": {"match": {"word": word}}})

    text = res['hits']['hits'][0]['_source']['text']
    res = es.search(
        index=index, doc_type=doc_type,
        body={"query": {"match": {"text": text}}})

    print(word)
    for hit in res['hits']['hits']:
        print('  %s' % hit['_source']['word'])
