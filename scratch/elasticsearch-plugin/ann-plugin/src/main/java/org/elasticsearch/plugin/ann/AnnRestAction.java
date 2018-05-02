package org.elasticsearch.plugin.ann;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class AnnRestAction extends BaseRestHandler {

    public static String NAME = "_ann";
    private final String NAME_SEARCH = "_search_ann";
    private final String NAME_INSERT = "_insert_ann";
    private final String NAME_CREATE = "_create_ann";

    // TODO: make these actual parameters.
    private final String HASHES_KEY = "hashes";
    private final Integer K1_DEFAULT = 99;     // Number of documents returned based on hashes only.
    private final Integer K2_DEFAULT = 10;     // Number of documents returned based on exact KNN.

    @Inject
    public AnnRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/{type}/{id}/" + NAME_SEARCH, this);
        controller.registerHandler(POST, NAME_INSERT, this);
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
        else if (restRequest.path().endsWith(NAME_INSERT))
            return handleInsertRequest(restRequest, client);
        else
            return handleCreateRequest(restRequest, client);
    }

    private RestChannelConsumer handleSearchRequest(RestRequest restRequest, NodeClient client) throws IOException {

        // TODO: the search request should be modified such that regular query options (e.g. _source: ["description"])
        // can be included. See Carrot2 examples and docs: https://github.com/carrot2/elasticsearch-carrot2/blob/master/doc/

        final Double nanosecondsInSecond = 1000000000.0;
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
        List<Double> baseVector = (List<Double>) baseSource.get("vector");
        timing.add(Tuple.tuple("Retrieving base document", (System.nanoTime() - timestamp) / nanosecondsInSecond));

        // Retrieve the documents with most matching hashes. https://stackoverflow.com/questions/10773581
        timestamp = System.nanoTime();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Integer> entry : baseHashes.entrySet()) {
            // TODO: using String.format() gives a forbidden APIs waring here.
            // String termKey = String.format("hashes.%s", entry.getKey());
            String termKey = HASHES_KEY + "." + entry.getKey();
            ((BoolQueryBuilder) queryBuilder).should(QueryBuilders.termQuery(termKey, entry.getValue()));
        }
        timing.add(Tuple.tuple("Building approximate query", (System.nanoTime() - timestamp) / nanosecondsInSecond));

        timestamp = System.nanoTime();
        SearchResponse approximateSearchResponse = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setSize(k1)
                .setExplain(false)
                .get();
        timing.add(Tuple.tuple("Executing approximate query", (System.nanoTime() - timestamp) / nanosecondsInSecond));

        // Compute exact KNN on the approximate neighbors.
        timestamp = System.nanoTime();
        SearchHits searchHits = approximateSearchResponse.getHits();
        List<Tuple<String, Double>> idsAndDistances = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            Map<String, Object> hitSource = hit.getSourceAsMap();
            @SuppressWarnings("unchecked")
            List<Double> hitVector = (List<Double>) hitSource.get("vector");
            idsAndDistances.add(Tuple.tuple(hit.getId(), euclideanDistance(baseVector, hitVector)));
        }
        timing.add(Tuple.tuple("Computing distances", (System.nanoTime() - timestamp) / nanosecondsInSecond));

        // Sort ids by the exact distance in ascending order.
        timestamp = System.nanoTime();
        idsAndDistances.sort(Comparator.comparing(Tuple::v2));
        List<Tuple<String, Double>> idsAndDistancesTopK = idsAndDistances.subList(0, k2);
        timing.add(Tuple.tuple("Sorting by distance", (System.nanoTime() - timestamp) / nanosecondsInSecond));

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("nearest_neighbors", idsAndDistancesTopK);
            builder.field("timing", timing);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    private RestChannelConsumer handleCreateRequest(RestRequest restRequest, NodeClient client) throws IOException {

        logger.info("params: " + restRequest.params().toString());
        logger.info("content: " + restRequest.hasContent());

        XContentParser xContentParser = XContentHelper.createParser(
                restRequest.getXContentRegistry(), restRequest.content(), restRequest.getXContentType());
        Map<String, Object> contentMap = xContentParser.mapOrdered();

        final String _index = (String) contentMap.get("_index");
        final String _type = (String) contentMap.get("_type");
        final String _id = (String) contentMap.get("_id");
        final String description = (String) contentMap.get("description");
        final Integer nbTables = (Integer) contentMap.get("nb_tables");
        final Integer nbBitsPerTable = (Integer) contentMap.get("nb_bits_per_table");
        final Integer nbDimensions = (Integer) contentMap.get("nb_dimensions");
        final String vectorSampleCSV = (String) contentMap.get("vector_sample_csv");

        // TODO: ask someone who actually knows Java how to simplify this parsing.
        int i = 0, j = 0;
        RealMatrix vectorSample = MatrixUtils.createRealMatrix(2 * nbTables * nbBitsPerTable, nbDimensions);
        for (String line : vectorSampleCSV.split("\n")) {
            for (String token: line.split(","))
                vectorSample.setEntry(i, j++, Double.parseDouble(token));
            j = 0; i += 1;
        }

        // Fit a set of normal hyperplanes from the given vectors
        LshModel lshModel = new LshModel(nbTables, nbBitsPerTable, nbDimensions, description);
        lshModel.fitFromVectorSample(vectorSample);

        IndexResponse indexResponse = client.prepareIndex(_index, _type, _id)
                .setSource(lshModel.getSerializable()).get();
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

    private RestChannelConsumer handleInsertRequest(RestRequest restRequest, NodeClient client) throws IOException {
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("did_it_work", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }


}
