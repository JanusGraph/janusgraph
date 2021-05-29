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

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversalDsl;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.net.InetAddress;
import java.util.Map;

/**
 * A simple implementation of an {@link Authenticator} that uses a {@link Graph} instance as a credential store.
 * Management of the credential store can be handled through the {@link CredentialTraversalDsl}.
 */
public class JanusGraphSimpleAuthenticator extends JanusGraphAbstractAuthenticator {

    protected SimpleAuthenticator simpleAuthenticator;

    @Override
    public void setup(final Map<String, Object> config) {
        super.setup(config);
        simpleAuthenticator = createSimpleAuthenticator();
        simpleAuthenticator.setup(config);
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return simpleAuthenticator.newSaslNegotiator(remoteAddress);
    }

    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        return simpleAuthenticator.authenticate(credentials);
    }

    protected SimpleAuthenticator createSimpleAuthenticator() {
        return new SimpleAuthenticator();
    }
}
