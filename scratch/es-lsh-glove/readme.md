
## Introduction

1. Use [GloVe: Global Vectors for Word Representations](https://nlp.stanford.edu/projects/glove/), specifically the 6B-50D vectors with 400K terms.
2. Run an exact KNN with cosine distance.
3. Run a very simple LSH on the Glove vectors, insert the hashes in ES as text documents. Try similarity search

## Results

Insert hashed Glove vectors to Elasticsearch and look them up using the hash text.

For example, if the hash vector is [0 1 0 1 1 1], the document has tokens ["0_0", "1_1", "2_0", "3_1", "4_1", "5_1"].

Documents look like this:

```
GET glove50/_search
{
  "size": 1,
  "query": {
    "term": {
      "word": "cat"
    }
  }
}
```

```
{
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 4,
    "max_score": 11.294465,
    "hits": [
      {
        "_index": "glove50",
        "_type": "word",
        "_id": "5450",
        "_score": 11.294465,
        "_source": {
          "text": "0_0 1_1 2_0 3_1 4_1 5_0 6_1 7_0 8_0 9_1 10_0 11_0 12_0 13_1 14_1 15_1 16_0 17_1 18_0 19_0 20_0 21_0 22_0 23_0 24_1 25_1 26_0 27_0 28_1 29_1 30_1 31_0 32_0 33_0 34_0 35_1 36_1 37_1 38_1 39_0 40_0 41_0 42_0 43_0 44_1 45_0 46_1 47_0 48_0 49_0 50_1 51_1 52_0 53_0 54_1 55_1 56_1 57_0 58_1 59_0 60_1 61_1 62_0 63_1 64_1 65_0 66_1 67_1 68_1 69_1 70_0 71_0 72_0 73_1 74_1 75_1 76_0 77_1 78_0 79_0 80_0 81_0 82_0 83_0 84_0 85_0 86_1 87_0 88_1 89_0 90_0 91_0 92_0 93_1 94_0 95_1 96_1 97_0 98_1 99_0 100_0 101_1 102_0 103_0 104_0 
          ...
          1001_1 1002_1 1003_1 1004_1 1005_0 1006_1 1007_0 1008_0 1009_0 1010_0 1011_1 1012_0 1013_1 1014_1 1015_0 1016_1 1017_0 1018_1 1019_1 1020_1 1021_0 1022_1 1023_0 ",
          "word": "cat"
        }
      }
    ]
  }
}
```

Similarity query results look like this:

```
(insight) alex@ltp:knn-python$ python glove_lsh_es_query.py glove50 word quantum
quantum
  quantum
  theory
  gravity
  relativity
  evolution
  dynamics
  particle
  computation
  mathematical
  molecular
```

```
(insight) alex@ltp:knn-python$ python glove_lsh_es_query.py glove50 word music
music
  music
  musical
  recording
  studio
  pop
  artists
  songs
  contemporary
  best
  well
```

```
(insight) alex@ltp:knn-python$ python glove_lsh_es_query.py glove50 word tennis
tennis
  tennis
  tournament
  golf
  volleyball
  wimbledon
  soccer
  open
  finals
  semi
  championships
```

```
(insight) alex@ltp:knn-python$ python glove_lsh_es_query.py glove50 word fox
fox
  fox
  show
  abc
  nbc
  shows
  cbs
  tv
  television
  cnn
  's
```

