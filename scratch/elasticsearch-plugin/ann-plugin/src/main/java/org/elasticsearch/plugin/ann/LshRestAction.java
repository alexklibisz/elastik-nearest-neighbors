package org.elasticsearch.plugin.ann;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class LshRestAction extends BaseRestHandler {

    public static String NAME = "_build_lsh";

    @Inject
    public LshRestAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, NAME, this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

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
        List<List<Float>> vectorSample = new ArrayList<>();
        String[] vectorLines = vectorSampleCSV.split("\n");
        for (String line : vectorLines) {
            List<Float> vector = new ArrayList<>();
            for (String value : line.split(","))
                vector.add(Float.parseFloat(value));
            vectorSample.add(vector);
        }

        System.out.println(_index);
        System.out.println(_type);
        System.out.println(_id);
        System.out.println(description);
        System.out.println(nbDimensions);
        System.out.println(nbTables);
        System.out.println(nbBitsPerTable);
        System.out.println(vectorSample);


        // Fit a set of normal hyperplanes from the given vectors
        LshModel lshModel = new LshModel(nbTables, nbBitsPerTable, nbDimensions);
        lshModel.fitFromVectorSample(vectorSample);
//        lshModel.toJSONString();

//        List<List<Float>> lshSamplesA = vectorSample.subList(0, nbBitsPerTable);
//        List<List<Float>> lshSamplesB = vectorSample.subList(nbBitsPerTable, nbBitsPerTable + nbBitsPerTable);
//        System.out.println(lshSamplesA.toString());
//        System.out.println(lshSamplesB.toString());

//        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
//                .setSource(jsonBuilder()
//                        .startObject()
//                        .field("user", "kimchy")
//                        .field("postDate", new Date())
//                        .field("message", "trying out Elasticsearch")
//                        .endObject()
//                ).get();
//        System.out.println(response.toString());
//
//        String json = "{" +
//                "\"user\":\"kimchy\"," +
//                "\"postDate\":\"2013-01-30\"," +
//                "\"message\":\"trying out Elasticsearch\"" +
//                "}";
//        response = client.prepareIndex("twitter", "tweet", "2")
//                .setSource(json, XContentType.JSON)
//                .get();
//        System.out.println(response.toString());

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("did_it_work", "yes");
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };

    }



}

