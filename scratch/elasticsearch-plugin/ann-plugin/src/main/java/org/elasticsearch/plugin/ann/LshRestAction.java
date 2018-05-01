package org.elasticsearch.plugin.ann;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

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
        int i = 0, j = 0;
        RealMatrix vectorSample = MatrixUtils.createRealMatrix(2 * nbTables * nbBitsPerTable, nbDimensions);
        for (String line : vectorSampleCSV.split("\n")) {
            for (String token: line.split(","))
                vectorSample.setEntry(i, j++, Double.parseDouble(token));
            j = 0; i += 1;
        }

        // Fit a set of normal hyperplanes from the given vectors
        LshModel lshModel = new LshModel(nbTables, nbBitsPerTable, nbDimensions);
        lshModel.fitFromVectorSample(vectorSample);

//        double [][] doubleArray = {{1., 2.}, {3., 4}};
//        JSONArray doubleJSONArray = new JSONArray(doubleArray);
//        System.out.println(doubleJSONArray.toString());



        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("did_it_work", "yes");
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };

    }



}

