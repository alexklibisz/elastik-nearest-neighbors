package org.elasticsearch.plugin.ingest.awesome;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class HelloRestAction extends BaseRestHandler {

    public static String NAME = "_hello";

    @Inject
    public HelloRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_hello", this);
    }

//    @Override
//    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
//        String clusterName = client.settings().get("cluster.name");
//        channel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentFactory.jsonBuilder().startObject().field("hello", "This is cluster " + clusterName).endObject()));
//    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String clusterName = client.settings().get("cluster.name");
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("Hello", "This is cluster " + clusterName);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }


}
