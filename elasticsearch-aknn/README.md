# Elasticsearch-Aknn

Elasticsearch plugin for approximate K-nearest-neighbor queries on dense vectors using locality sensitive hashing.

## Usage

### Build and Install

For now, see commands in `testplugin.sh` script.

### Create Aknn Model

### Index a new Vector

### Nearest-neighbors Query

## TODO

- Document API endpoints in the README. There are only three, and they are already implicitly documented in the Python scripts in the benchmarks directory above this directory.
- Clean up the many different conversions from JSON lists of lists of floating point numbers, to Java `List<List<Double>>`, to Java `double[][]`, to Apache Commons Math `RealMatrix`.
- Clean up code to pass default compilation checks for Elasticsearch gradle plugin.
- Add abstractions that remove some of the bloat from `AknnRestAction.java` class.
- Consider switching the `_aknn_index` endpoint to an ingest processor. When inserting large batches, the POST request can take tens of seconds. E.g., for 10K new 300-dimensional vectors, it takes ~25 seconds. 
- Enforce an explicit mapping for new Aknn models stored in Elasticsearch. For example, the continuous hyperplanes should not be indexed. 
- Enforce an explicit mapping for new vector documents stored in Elasticsearch. For example, the continuous vectors should not be indexed.
- Cache the Aknn model by its URI, so that it doesn't have to be requested and deserialized for every new batch of documents. Perhaps add a `bust_cache` flag to the `_aknn_index` endpoint which would force the server to re-GET the Aknn model document.
