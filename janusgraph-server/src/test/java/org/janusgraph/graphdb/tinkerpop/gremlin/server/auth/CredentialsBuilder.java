// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_TOKEN;

public class CredentialsBuilder {

    final Map<String, String> credentials = new HashMap<>();

    public static CredentialsBuilder build() {
        return new CredentialsBuilder();
    }

    public CredentialsBuilder user(final String username) {
        credentials.put(PROPERTY_USERNAME, username);
        return this;
    }

    public CredentialsBuilder password(final String password) {
        credentials.put(PROPERTY_PASSWORD, password);
        return this;
    }

    public CredentialsBuilder enableTokenGeneration() {
        credentials.put(PROPERTY_GENERATE_TOKEN, "true");
        return this;
    }

    public CredentialsBuilder token(final String token) {
        credentials.put(PROPERTY_TOKEN, new String(Base64.getUrlDecoder().decode(token)));
        return this;
    }

    public Map<String, String> create() {
        return credentials;
    }
}
