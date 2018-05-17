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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;

public class AknnSimpleIT extends ESIntegTestCase {

    private Client client;
    private RestClient restClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = client();
        restClient = getRestClient();
    }

    /**
     * Test that the plugin was installed correctly by hitting the _cat/plugins endpoint.
     * @throws IOException
     */
    public void testPluginInstallation() throws IOException {
        Response response = restClient.performRequest("GET", "_cat/plugins");
        String body = EntityUtils.toString(response.getEntity());
        logger.info(body);
        assertTrue(body.contains("elasticsearch-aknn"));
    }

}
