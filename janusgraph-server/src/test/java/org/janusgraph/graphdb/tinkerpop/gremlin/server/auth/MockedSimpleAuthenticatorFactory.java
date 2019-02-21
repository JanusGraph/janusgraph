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

import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.janusgraph.core.JanusGraph;

import java.util.Map;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;

public class MockedSimpleAuthenticatorFactory implements MockedJanusGraphAuthenticatorFactory {

    public JanusGraphAbstractAuthenticator createInitializedAuthenticator(final Map<String, Object> config,
                                                                          final JanusGraph graph) {
        final JanusGraphSimpleAuthenticator authenticator = createMockBuilder(JanusGraphSimpleAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createSimpleAuthenticator")
            .createMock();
        final SimpleAuthenticator sa = createMock(SimpleAuthenticator.class);
        expect(authenticator.createSimpleAuthenticator()).andReturn(sa);
        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        sa.setup(config);
        replay(authenticator);
        return authenticator;
    }
}
