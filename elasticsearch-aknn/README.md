# Elasticsearch-Aknn

Elasticsearch plugin for approximate K-nearest-neighbor queries on dense vectors 
using locality sensitive hashing.

This plugin is still very much a work in progress. See the `testplugin.sh`
script for an outline of building/installing the plugin.

## TODO

1. Implement integration tests. It looks like Elasticsearch has some nice
integration testing functionality. I initially didn't have time to learn it 
while I was prototyping.
2. Add proper error checking and error responses to the endpoints to prevent
silent/ambiguous errors. For example, Elasticsearch prevents lowercase index
names and fails to index such a document, but the endpoint still returns 200.
3. Clean up the JSON <-> POJO serialization and deserialization, especially
the conversion of JSON lists of lists to Java `List<List<double>>` to 
Java `Double [][]` to `RealMatrix`.
4. Enforce an explicit mapping and types for new Aknn LSH models. For example, the LSH
hyperplanes should not be indexed and can likely be stored as `half_float` / Java `float`)
to save space / network latency.
5. Enforce an explicit mapping and types for `_aknn_vector` and `_aknn_hashes`
entries. For example, `_aknn_vector` should not be indexed and can likley be
stored as a `half_float` / Java `float`.
6. Determine a proper place for defining/changing plugin configurations. For
example, the name of the vector and hashes items.