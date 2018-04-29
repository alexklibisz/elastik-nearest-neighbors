package org.elasticsearch.plugin.ingest.awesome;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class HelloRestAction extends BaseRestHandler {

    public static String NAME = "_search_ann";

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

        // Rough outline...
        // Retrieve document by its ID.
        // Execute an OR query on its hashes to get any documents with matching hashes.
        // Compute the true nearest neighbors on those documents.
        // Return documents.

        // Parse request parameters.
        final String index = restRequest.param("index");
        final String type = restRequest.param("type");
        final String id = restRequest.param("id");

        // Retrieve the document specified by index/type/id.
        GetResponse getResponse = client.prepareGet(index, type, id).get();
        Map<String, Object> source = getResponse.getSource();

        logger.info(getResponse.getSource().toString());
        logger.info(source.get("h1"));
        logger.info(source.get("h2"));
        logger.info(source.get("h3"));

        // Run a bool query based on the hashes.
        // In Kibana it looks like this:
//        GET /hashed_vectors/hashed_vector/_search
//        {
//            "query": {
//            "bool" : {
//                "should" : [
//                { "term" : { "h1" : 110 }},
//                { "term" : { "h2" : 110 }},
//                { "term" : { "h3" : 100 }}
//      ],
//                "minimum_should_match" : 1,
//                        "boost" : 1.0
//            }
//        }
//        }


        SearchResponse searchResponse = client
                .prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery() TODO...
                .setFrom(0)
                .setSize(10)
                .setExplain(false)
                .get();

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("search_response", searchResponse);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }


}
