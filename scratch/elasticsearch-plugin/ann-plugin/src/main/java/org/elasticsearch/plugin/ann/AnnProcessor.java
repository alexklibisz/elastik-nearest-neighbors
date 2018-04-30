/*
 * Copyright [2017] [alex]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.elasticsearch.plugin.ann;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class AnnProcessor extends AbstractProcessor {

    public static final String TYPE = "ann_processor";
    private static final String VECTOR_FIELD_NAME = "vector_field";
    private static final String HASHES_FIELD_NAME = "hashes_field";
    private static final String VECTOR_FIELD_DEFAULT = "vector";
    private static final String HASHES_FIELD_DEFAULT = "hashes";

    private final String vectorField;
    private final String hashesField;

    public AnnProcessor(String tag, String vectorField, String hashesField) throws IOException {
        super(tag);
        this.vectorField = vectorField;
        this.hashesField = hashesField;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        @SuppressWarnings("unchecked")
        List<Double> content = ingestDocument.getFieldValue(vectorField, List.class);
        ingestDocument.setFieldValue(hashesField, "TODO: hash the vector via LSH");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public AnnProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config)
                throws Exception {

            String vectorField = readStringProperty(TYPE, tag, config, VECTOR_FIELD_NAME, VECTOR_FIELD_DEFAULT);
            String hashesField = readStringProperty(TYPE, tag, config, HASHES_FIELD_NAME, HASHES_FIELD_DEFAULT);

            // TODO: what is the tag?
//            System.out.println("tag: " + tag);
//            System.out.println("config: " + config.toString());
//            System.out.println("vectorField: " + vectorField);
//            System.out.println("hashesField: " + hashesField);

            return new AnnProcessor(tag, vectorField, hashesField);
        }
    }
}
