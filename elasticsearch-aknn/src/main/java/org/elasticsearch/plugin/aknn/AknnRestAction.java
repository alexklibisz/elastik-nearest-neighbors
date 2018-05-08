package org.elasticsearch.plugin.aknn;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class AknnRestAction extends BaseRestHandler {

    public static String NAME = "_aknn";
    private final String NAME_SEARCH = "_aknn_search";
    private final String NAME_INDEX = "_aknn_index";
    private final String NAME_CREATE = "_aknn_create";

    // TODO: make these actual parameters.
    private final String HASHES_KEY = "_aknn_hashes";
    private final String VECTOR_KEY = "_aknn_vector";
    private final Integer K1_DEFAULT = 99;     // Number of documents returned based on hashes only.
    private final Integer K2_DEFAULT = 10;     // Number of documents returned based on exact KNN.

    private final Double NANOSECONDS_PER_SECOND = 1000000000d;
    private final Double NANOSECONDS_PER_MILLISECOND = 1000000d;

    @Inject
    public AknnRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/{type}/{id}/" + NAME_SEARCH, this);
        controller.registerHandler(POST, NAME_INDEX, this);
        controller.registerHandler(POST, NAME_CREATE, this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static Double euclideanDistance(List<Double> A, List<Double> B) {
        Double squaredDistance = 0.;
        for (Integer i = 0; i < A.size(); i++)
            squaredDistance += Math.pow(A.get(i) - B.get(i), 2);
        return Math.sqrt(squaredDistance);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.path().endsWith(NAME_SEARCH))
            return handleSearchRequest(restRequest, client);
        else if (restRequest.path().endsWith(NAME_INDEX))
            return handleIndexRequest(restRequest, client);
        else
            return handleCreateRequest(restRequest, client);
    }

    @SuppressWarnings("unchecked")
    private RestChannelConsumer handleSearchRequest(RestRequest restRequest, NodeClient client) throws IOException {

        Long timestamp = 0L;
        List<Tuple<String, Double>> timing = new ArrayList<>();

        // Parse request parameters.
        final String index = restRequest.param("index");
        final String type = restRequest.param("type");
        final String id = restRequest.param("id");
        final Integer k1 = Integer.parseInt(restRequest.param("k1", K1_DEFAULT.toString()));
        final Integer k2 = Integer.parseInt(restRequest.param("k2", K2_DEFAULT.toString()));

        // Retrieve the document specified by index/type/id.
        timestamp = System.nanoTime();
        GetResponse baseGetResponse = client.prepareGet(index, type, id).get();
        Map<String, Object> baseSource = baseGetResponse.getSource();

        @SuppressWarnings("unchecked")
        Map<String, Integer> baseHashes = (Map<String, Integer>) baseSource.get(HASHES_KEY);

        @SuppressWarnings("unchecked")
        List<Double> baseVector = (List<Double>) baseSource.get(VECTOR_KEY);
        timing.add(Tuple.tuple("Retrieving base document", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        // Retrieve the documents with most matching hashes. https://stackoverflow.com/questions/10773581
        timestamp = System.nanoTime();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Integer> entry : baseHashes.entrySet()) {
             String termKey = String.format("%s.%s", HASHES_KEY, entry.getKey());
            ((BoolQueryBuilder) queryBuilder).should(QueryBuilders.termQuery(termKey, entry.getValue()));
        }
        timing.add(Tuple.tuple("Building approximate query", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        timestamp = System.nanoTime();
        SearchResponse approximateSearchResponse = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSize(k1)
                .setExplain(false)
                .get();
        timing.add(Tuple.tuple("Executing approximate query", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        // Compute exact KNN on the approximate neighbors.
        // Recreate the SearchHit structure, but remove the vector and hashes.
        timestamp = System.nanoTime();
        List<Map<String, Object>> modifiedSortedHits = new ArrayList<>();
        for (SearchHit hit: approximateSearchResponse.getHits()) {
            Map<String, Object> hitSource = hit.getSourceAsMap();
            List<Double> hitVector = (List<Double>) hitSource.get(VECTOR_KEY);
            hitSource.remove(VECTOR_KEY);
            hitSource.remove(HASHES_KEY);
            modifiedSortedHits.add(new HashMap<String, Object>() {{
                put("_index", hit.getIndex());
                put("_id", hit.getId());
                put("_type", hit.getType());
                put("_score", euclideanDistance(baseVector, hitVector));
                put("_source", hitSource);
            }});
        }
        timing.add(Tuple.tuple("Computing distances", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        timestamp = System.nanoTime();
        modifiedSortedHits.sort(Comparator.comparingDouble(x -> (Double) x.get("_score")));
        timing.add(Tuple.tuple("Sorting by distance", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("took", 0);
            builder.field("timed_out", false);
            builder.startObject("hits");
            builder.field("total", k2);
            builder.field("max_score", 0);
            builder.field("hits", modifiedSortedHits.subList(0, k2));
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    @SuppressWarnings("unchecked")
    private RestChannelConsumer handleCreateRequest(RestRequest restRequest, NodeClient client) throws IOException {

        logger.info("params: " + restRequest.params().toString());
        logger.info("content: " + restRequest.hasContent());

        XContentParser xContentParser = XContentHelper.createParser(
                restRequest.getXContentRegistry(), restRequest.content(), restRequest.getXContentType());
        Map<String, Object> contentMap = xContentParser.mapOrdered();
        Map<String, Object> sourceMap = (Map<String, Object>) contentMap.get("_source");

        final String _index = (String) contentMap.get("_index");
        final String _type = (String) contentMap.get("_type");
        final String _id = (String) contentMap.get("_id");
        final String description = (String) sourceMap.get("_aknn_description");
        final Integer nbTables = (Integer) sourceMap.get("_aknn_nb_tables");
        final Integer nbBitsPerTable = (Integer) sourceMap.get("_aknn_bits_per_table");
        final Integer nbDimensions = (Integer) sourceMap.get("_aknn_nb_dimensions");
        final List<List<Double>> vectorSample = (List<List<Double>>) contentMap.get("_aknn_vector_sample");

        // Fit a set of normal hyperplanes from the given vectors
        LshModel lshModel = new LshModel(nbTables, nbBitsPerTable, nbDimensions, description);
        lshModel.fitFromVectorSample(vectorSample);

        IndexResponse indexResponse = client.prepareIndex(_index, _type, _id)
                .setSource(lshModel.toMap()).get();
        logger.info("indexResponse: " + indexResponse.toString());

        GetResponse getResponse = client.prepareGet(_index, _type, _id).get();
        logger.info("getResponse: " + getResponse.toString());

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("source_nb_bytes", getResponse.getSourceAsBytes().length);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    @SuppressWarnings("unchecked")
    private RestChannelConsumer handleIndexRequest(RestRequest restRequest, NodeClient client) throws IOException {

        Long timestamp = 0L;
        List<Tuple<String, Double>> timing = new ArrayList<>();

        timestamp = System.nanoTime();
        XContentParser xContentParser = XContentHelper.createParser(
                restRequest.getXContentRegistry(), restRequest.content(), restRequest.getXContentType());
        Map<String, Object> contentMap = xContentParser.mapOrdered();

        final String _index = (String) contentMap.get("_index");
        final String _type = (String) contentMap.get("_type");
        final String _ann_uri = (String) contentMap.get("_aknn_uri");
        final List<Map<String, Object>> docs = (List<Map<String, Object>>) contentMap.get("_aknn_docs");
        logger.info(String.format("Received %d docs for indexing", docs.size()));
        timing.add(Tuple.tuple("Parsing request", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        // Get the Aknn document.
        timestamp = System.nanoTime();
        logger.info(String.format("Getting AKNN model stored at %s", _ann_uri));
        String[] annURITokens = _ann_uri.split("/");
        GetResponse annGetResponse = client.prepareGet(annURITokens[0], annURITokens[1], annURITokens[2]).get();
        logger.info("Done");
        timing.add(Tuple.tuple("Retrieving AKNN model", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        // Instantiate LSH from the source map.
        timestamp = System.nanoTime();
        logger.info("Parsing AKNN model");
        LshModel lshModel = LshModel.fromMap(annGetResponse.getSourceAsMap());
        logger.info("Done");
        timing.add(Tuple.tuple("Parsing AKNN model", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        // TODO: check if the index exists.. If it does not, create a mapping which does not index continuous values.
        // This is rather low priority, as I tried it via Python and it doesn't make much difference.

        // Prepare documents for batch indexing.
        logger.info("Preparing documents for bulk indexing");
        timestamp = System.nanoTime();
        BulkRequestBuilder bulkIndexRequest = client.prepareBulk();
        for (Map<String, Object> doc: docs) {
//            long timestampLoop = System.nanoTime();
//            logger.info("Preparing document with ID: " + (String) doc.get("_id"));
            Map<String, Object> source = (Map<String, Object>) doc.get("_source");
            List<Double> vector = (List<Double>) source.get(VECTOR_KEY);
//            System.out.println(System.nanoTime() - timestampLoop);
//            timestampLoop = System.nanoTime();
            List<Long> hashes = lshModel.getVectorHashes(vector);
//            System.out.println(System.nanoTime() - timestampLoop);
//            timestampLoop = System.nanoTime();
            Map<String, Long> hashesAsMap = new HashMap<>();
            for (Integer i = 0; i < hashes.size(); i++)
                hashesAsMap.put(i.toString(), hashes.get(i));
            source.put(HASHES_KEY, hashesAsMap);
//            System.out.println(System.nanoTime() - timestampLoop);
            bulkIndexRequest.add(client
                    .prepareIndex(_index, _type, (String) doc.get("_id"))
                    .setSource(source));
        }
        timing.add(Tuple.tuple("Preparing documents", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        logger.info("Executing bulk indexing");
        timestamp = System.nanoTime();
        BulkResponse bulkIndexResponse = bulkIndexRequest.get();
        logger.info("Done");
        timing.add(Tuple.tuple("Indexing documents", (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        if (bulkIndexResponse.hasFailures())
            logger.error(String.format("Indexing failed after %.8f seconds with message: %s",
                    (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND,
                    bulkIndexResponse.buildFailureMessage()));
        else
            logger.info(String.format("Indexed %d docs in %.8f seconds", docs.size(),
                    (System.nanoTime() - timestamp) / NANOSECONDS_PER_SECOND));

        for (Tuple<String, Double> t: timing) {
            System.out.println(String.format("%s: %f", t.v1(), t.v2()));
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("nb_docs_index", docs.size());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }


}
