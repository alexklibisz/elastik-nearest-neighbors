"""Read cleaned twitter statuses from disk and insert them
to local elasticsearch instance."""
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers
import pdb

if __name__ == "__main__":
    es = Elasticsearch()

    # # ~45 minutes to insert ~498K documents.
    # for line in tqdm(open("tweet_texts.txt")):
    #     es.index(index="tweets", doc_type="tweet", body={"text": line})

    # ~40 seconds to bulk insert in batches of 10000 documents.
    actions = []

    for i, line in tqdm(enumerate(open("tweet_texts.txt"))):

        actions.append({
            "_index": "tweets2",
            "_type": "tweet",
            "_id": i,
            "_source": {
                "text": line
            }
        })

        if len(actions) == 10000:
            helpers.bulk(es, actions)
            actions = []

    helpers.bulk(es, actions)
    actions = []
