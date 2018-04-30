package org.elasticsearch.plugin.ingest.awesome;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class HelloRestAction extends BaseRestHandler {

    public static String NAME = "_search_ann";
    private final String HASHES_KEY = "hashes";
    private final Integer K1_DEFAULT = 100;     // Number of documents returned based on hashes only.
    private final Integer K2_DEFAULT = 10;      // Number of documents returned based on exact KNN.

    @Inject
    public HelloRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/{index}/{type}/{id}/" + NAME, this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        // Parse request parameters.
        final String index = restRequest.param("index");
        final String type = restRequest.param("type");
        final String id = restRequest.param("id");
        final Integer k1 = Integer.parseInt(restRequest.param("k1", K1_DEFAULT.toString()));
        final Integer k2 = Integer.parseInt(restRequest.param("k2", K2_DEFAULT.toString()));

        // Retrieve the document specified by index/type/id.
        GetResponse getResponse = client.prepareGet(index, type, id).get();
        Map<String, Object> source = getResponse.getSource();

        @SuppressWarnings("unchecked")
        Map<String, Integer> hashes = (Map<String, Integer>) source.get(HASHES_KEY);

        // Retrieve the documents with most matching hashes.
        // https://stackoverflow.com/questions/10773581
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (Map.Entry<String, Integer> entry : hashes.entrySet()) {
            // TODO: using String.format() gives a forbidden APIs waring here.
            // String termKey = String.format("hashes.%s", entry.getKey());
            String termKey = HASHES_KEY + "." + entry.getKey();
            ((BoolQueryBuilder) queryBuilder).should(QueryBuilders.termQuery(termKey, entry.getValue()));
        }

        SearchResponse approximateNeighborsResponse = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setSize(k1)
                .setExplain(false)
                .get();

        // Compute exact KNN on the approximate neighbors.

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("search_response", approximateNeighborsResponse);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }


}
