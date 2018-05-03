# Elasticsearch-Aknn

Elasticsearch plugin for approximate K-nearest-neighbor queries on dense vectors.

## TODO

1. Document API endpoints. They are already implicitly documented in the benchmarks directory above this directory.
2. Clean up the many different conversions from JSON lists of lists of floating point numbers, to Java `List<List<Double>>`, to Java `double[][]`, to Apache Commons Math `RealMatrix`.
3. Clean up code to pass default compilation checks for Elasticsearch gradle plugin.
4. Add abstractions that remove some of the bloat from `AknnRestAction.java` class.
4. Consider switching the `_aknn_index` endpoint to an ingest processor. 
