
## Metrics

- 45 minutes to insert 498K documents as individual requests.
- 40 seconds to insert 498K documents in batches of 10K documents.

## Results

Simple query in Kibana:

```
GET tweets/_search 
{
  "query": {
    "match": {
      "text": "Fake news media"
    }
  }
}
```

```
{
  "took": 17,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 21021,
    "max_score": 16.087315,
    "hits": [
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "pGD83mIBXudwSMXjAVz3",
        "_score": 16.087315,
        "_source": {
          "text": "Fake news regularly appears in the media: https://t.co/4ty4SJAUHt\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "wWQR32IBXudwSMXjf0-r",
        "_score": 15.683937,
        "_source": {
          "text": "Fake news media covering up ANTIFA terrorist attack https://t.co/TUNv2saNdq\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "_WUZ32IBXudwSMXjRr_c",
        "_score": 15.528734,
        "_source": {
          "text": "https://t.co/A9li9olSUA This is from Thursday. MEDIA LIES. FAKE NEWS\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "AWD93mIBXudwSMXj-bqt",
        "_score": 15.437178,
        "_source": {
          "text": """
"The fake news media has been spreading fake news unbelievably. But we're making America great again. Right, folks? I think so" #SOTU

"""
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "02QP32IBXudwSMXj5gPx",
        "_score": 15.33066,
        "_source": {
          "text": "Cernovich reads hater articles from the fake news media https://t.co/gkZvcsuoky\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "lWIH32IBXudwSMXj4Yeh",
        "_score": 15.300294,
        "_source": {
          "text": "Fake News Panic and the Silencing of Dissident Media’ https://t.co/ZBUL6jE9JY\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "BmD63mIBXudwSMXjPgl5",
        "_score": 15.279158,
        "_source": {
          "text": "Cernovich reads hater articles from the fake news media https://t.co/gkZvcsuoky\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "MmED32IBXudwSMXjNrHy",
        "_score": 15.279158,
        "_source": {
          "text": "Fake and failing news media.@Newsweek #FakeNews 👉https://t.co/ey1yzYBK16https://t.co/xa1XgWCO9h\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "omUX32IBXudwSMXjo3Ij",
        "_score": 15.14844,
        "_source": {
          "text": "@seanhannity @nytimes @FBI Speaking of the fake news media... https://t.co/s4DpS9fIyU\n"
        }
      },
      {
        "_index": "tweets",
        "_type": "tweet",
        "_id": "IGMM32IBXudwSMXjOlXv",
        "_score": 14.964441,
        "_source": {
          "text": "Trump: Mainstream Media Should Compete for Fake News Trophy - https://t.co/JLV7Vn43oE   #CNNisFakeNews 🏆\n"
        }
      }
    ]
  }
}
```