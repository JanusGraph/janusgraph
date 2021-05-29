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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

public class BasicAuthHttpClientConfigCallback implements HttpClientConfigCallback {

    private final CredentialsProvider credentialsProvider;

    public BasicAuthHttpClientConfigCallback(final String realm, final String username, final String password) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(username), "HTTP Basic Authentication: username must be provided");
        Preconditions.checkArgument(StringUtils.isNotEmpty(password), "HTTP Basic Authentication: password must be provided");

        credentialsProvider = new BasicCredentialsProvider();

        final AuthScope authScope;
        if (StringUtils.isNotEmpty(realm)) {
            authScope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, realm, AuthScope.ANY_SCHEME);
        } else {
            authScope = AuthScope.ANY;
        }
        credentialsProvider.setCredentials(authScope, new UsernamePasswordCredentials(username, password));
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        return httpClientBuilder;
    }
}
