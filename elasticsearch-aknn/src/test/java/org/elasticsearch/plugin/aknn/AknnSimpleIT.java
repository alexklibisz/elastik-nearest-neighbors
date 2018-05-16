/*
 * Copyright [2018] [Alex Klibisz]
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

package org.elasticsearch.plugin.aknn;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class AknnSimpleIT extends ESIntegTestCase {

    private Client client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = ESIntegTestCase.client();
        client.prepareIndex("twitter", "tweet", "1")
            .setSource(jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject()
            ).get();
    }

    public void testTweet1() {
        GetResponse gr = client.prepareGet("twitter", "tweet", "1").get();
        assertEquals(gr.getSource().get("user"), "kimchy");
    }

    public void testTweet2() {
        GetResponse gr = client.prepareGet("twitter", "tweet", "1").get();
        assertEquals(gr.getSource().get("message"), "trying out Elasticsearch");
    }

}
