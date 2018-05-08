
# Elasticsearch-Aknn Benchmarks

## Preprocess Glove Vectors

```
./glove_download.sh
python glove_preprocess.py glove.840B.300d.txt > glove.840B.300d.docs.txt
```

## Create a new AKNN model from Glove vectors

```
python -m aknn -e http://localhost:9200 create glove.840B.300d.docs.txt \
	--index aknn_models \
	--type aknn_model \
	--id glove_840B_300d \
	--description "Aknn model for glove.840B.300d.txt" \
	--dimensions 300 \
	--tables 32 \
	--bits 16
```

## Index Glove vectors

```
python -m aknn -e http://localhost:9200 index \
	glove.840B.300d.docs.txt \
	index_metrics.csv \
	--aknn_uri aknn_models/aknn_model/glove_840B_300d \
	--index glove_word_vectors \
	--type glove_word_vector \
	--batch_size 1000 \
	--count 10000

```

## Test concurrent search queries

```
python -m aknn -h http://localhost:9200,http://localhost:9202 search \
	--index glove_word_vectors \
	--type glove_word_vector \
	--time 5 \
	--threads 10
```