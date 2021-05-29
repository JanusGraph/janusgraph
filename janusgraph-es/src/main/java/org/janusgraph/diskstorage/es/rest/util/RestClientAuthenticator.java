// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es.rest.util;

import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;

import java.io.IOException;

/**
 * <p>
 * A class instance implementing this interface may customize some aspects of
 * JanusGraph Elasticsearch REST client to implement a custom authentication method.
 * </p>
 * <p>
 * An implementation must provide a constructor that accepts a String[]. It will receive
 * the value(s) from {@link org.janusgraph.diskstorage.es.ElasticSearchIndex#ES_HTTP_AUTHENTICATOR_ARGS}.
 * If no authenticator arguments are provided, a zero-size array is passed to the
 * constructor. The constructor must not perform any blocking operations.
 * </p>
 */
public interface RestClientAuthenticator extends HttpClientConfigCallback, RequestConfigCallback {

    /**
     * Initializes the authenticator. This method may perform the blocking I/O operations if needed.
     * @throws IOException in case there was an exception during I/O operations.
     */
    void init() throws IOException;

}
