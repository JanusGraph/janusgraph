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

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

import java.net.InetAddress;
import java.util.Map;

/**
 * A wrapper authenticator that instantiates A JanusGraphSimpleAuthenticator (for Sasl)
 * and an HMACAuthenticator (for http)
 * @author Keith Lohnes lohnesk@gmail.com
 */
public class SaslAndHMACAuthenticator extends JanusGraphAbstractAuthenticator {

    private static final String ILLEGAL_STATE_MESSAGE =
        "This exception is likely due to a misconfiguration. Try using the SaslAndHMACAuthenticationHandler as the " +
        "authenticationHandler in the server authentication configuration";

    private static final String INCORRECT_CLASS_USAGE = "SaslAndHMACAuthenticator is a wrapper for two auths and should not be called directly.";

    private JanusGraphSimpleAuthenticator janusSimpleAuthenticator;
    private HMACAuthenticator hmacAuthenticator ;

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        throw new RuntimeException(INCORRECT_CLASS_USAGE);
    }

    public SaslNegotiator newSaslNegotiator() {
        throw new RuntimeException(INCORRECT_CLASS_USAGE);
    }

    @Override
    public void setup(final Map<String,Object> config) {
        this.janusSimpleAuthenticator = createSimpleAuthenticator();
        this.hmacAuthenticator = createHMACAuthenticator();
        super.setup(config);
        this.janusSimpleAuthenticator.simpleAuthenticator = this.janusSimpleAuthenticator.createSimpleAuthenticator();
        this.janusSimpleAuthenticator.simpleAuthenticator.setup(config);
        this.hmacAuthenticator.setup(config);
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        throw new IllegalStateException(ILLEGAL_STATE_MESSAGE);
    }

    public JanusGraphSimpleAuthenticator getSimpleAuthenticator() {
        return this.janusSimpleAuthenticator;
    }

    public HMACAuthenticator getHMACAuthenticator() {
        return this.hmacAuthenticator;
    }

    protected JanusGraphSimpleAuthenticator createSimpleAuthenticator() {
        return new JanusGraphSimpleAuthenticator();
    }

    protected HMACAuthenticator createHMACAuthenticator() {
        return new HMACAuthenticator();
    }


}
