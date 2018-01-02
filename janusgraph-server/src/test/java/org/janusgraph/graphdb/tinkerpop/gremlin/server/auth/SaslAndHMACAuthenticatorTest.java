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

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator.CONFIG_CREDENTIALS_DB;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraph;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.easymock.EasyMockSupport;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.junit.Test;

public class SaslAndHMACAuthenticatorTest extends EasyMockSupport {

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiator() {
        new SaslAndHMACAuthenticator().newSaslNegotiator();
    }

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiatorInet() {
        final InetAddress inet = createMock(InetAddress.class);
        new SaslAndHMACAuthenticator().newSaslNegotiator(inet);
    }

    @Test(expected = IllegalStateException.class)
    public void testAuthenticate() throws AuthenticationException {
        new SaslAndHMACAuthenticator().authenticate(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetupNullConfig() {
        final SaslAndHMACAuthenticator authenticator = new SaslAndHMACAuthenticator();
        authenticator.setup(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupNoCredDb() {
        final SaslAndHMACAuthenticator authenticator = new SaslAndHMACAuthenticator();
        authenticator.setup(new HashMap<String, Object>());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupEmptyNoUserDefault() {
        final SaslAndHMACAuthenticator authenticator = createMockBuilder(SaslAndHMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        authenticator.setup(configMap);
    }

    @Test(expected=IllegalStateException.class)
    public void testSetupEmptyCredGraphNoPassDefault() {
        final SaslAndHMACAuthenticator authenticator = createMockBuilder(SaslAndHMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        authenticator.setup(configMap);
    }

    @Test
    public void testSetupEmptyCredGraphNoUserIndex() {
        final SaslAndHMACAuthenticator authenticator = createMockBuilder(SaslAndHMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .addMockedMethod("createSimpleAuthenticator")
            .addMockedMethod("createHMACAuthenticator")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final JanusGraphSimpleAuthenticator janusSimpleAuthenticator = createMock(JanusGraphSimpleAuthenticator.class);
        final HMACAuthenticator hmacAuthenticator = createMock(HMACAuthenticator.class);
        final SimpleAuthenticator simpleAuthenticator = createMock(SimpleAuthenticator.class);
        final Transaction tx = createMock(Transaction.class);
        final PropertyKey pk = createMock(PropertyKey.class);
        final PropertyKeyMaker pkm = createMock(PropertyKeyMaker.class);
        final JanusGraphManagement.IndexBuilder indexBuilder = createMock(JanusGraphManagement.IndexBuilder.class);
        final JanusGraphIndex index = createMock(JanusGraphIndex.class);
        final PropertyKey[] pks = {pk};

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(authenticator.createSimpleAuthenticator()).andReturn(janusSimpleAuthenticator);
        expect(authenticator.createHMACAuthenticator()).andReturn(hmacAuthenticator);
        hmacAuthenticator.setup(configMap);
        expectLastCall();
        expect(janusSimpleAuthenticator.createSimpleAuthenticator()).andReturn(simpleAuthenticator);
        simpleAuthenticator.setup(configMap);
        expectLastCall();

        expect(credentialGraph.findUser("user")).andReturn(null);
        expect(credentialGraph.createUser(eq("user"), eq("pass"))).andReturn(null);
        expect(graph.openManagement()).andReturn(mgmt).times(2);
        expect(graph.tx()).andReturn(tx);
        expect(index.getFieldKeys()).andReturn(pks);
        expect(index.getIndexStatus(eq(pk))).andReturn(SchemaStatus.ENABLED);

        tx.rollback();
        expectLastCall();

        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(false);
        expect(mgmt.makePropertyKey(PROPERTY_USERNAME)).andReturn(pkm);
        expect(pkm.dataType(eq(String.class))).andReturn(pkm);
        expect(pkm.cardinality(Cardinality.SINGLE)).andReturn(pkm);
        expect(pkm.make()).andReturn(pk);
        expect(mgmt.buildIndex(eq("byUsername"), eq(Vertex.class))).andReturn(indexBuilder);
        expect(mgmt.getGraphIndex(eq("byUsername"))).andReturn(index);
        expect(indexBuilder.addKey(eq(pk))).andReturn(indexBuilder);
        expect(indexBuilder.unique()).andReturn(indexBuilder);
        expect(indexBuilder.buildCompositeIndex()).andReturn(index);

        mgmt.commit();
        expectLastCall();

        mgmt.rollback();
        expectLastCall();

        replayAll();

        authenticator.setup(configMap);
    }

    @Test
    public void testPassEmptyCredGraphUserIndex() {
        final SaslAndHMACAuthenticator authenticator = createMockBuilder(SaslAndHMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .addMockedMethod("createSimpleAuthenticator")
            .addMockedMethod("createHMACAuthenticator")
            .createMock();

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final HMACAuthenticator hmacAuthenticator = createMock(HMACAuthenticator.class);
        final JanusGraphSimpleAuthenticator janusSimpleAuthenticator = createMock(JanusGraphSimpleAuthenticator.class);
        final SimpleAuthenticator simpleAuthenticator = createMock(SimpleAuthenticator.class);
        final Transaction tx = createMock(Transaction.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(authenticator.createSimpleAuthenticator()).andReturn(janusSimpleAuthenticator);
        expect(authenticator.createHMACAuthenticator()).andReturn(hmacAuthenticator);
        hmacAuthenticator.setup(configMap);
        expectLastCall();
        expect(janusSimpleAuthenticator.createSimpleAuthenticator()).andReturn(simpleAuthenticator);
        simpleAuthenticator.setup(configMap);
        expectLastCall();
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        expect(credentialGraph.findUser("user")).andReturn(null);
        expect(credentialGraph.createUser(eq("user"), eq("pass"))).andReturn(null);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        tx.rollback();
        expectLastCall();
        replayAll();

        authenticator.setup(configMap);
    }

    @Test
    public void testSetupDefaultUserNonEmptyCredGraph() {
        final SaslAndHMACAuthenticator authenticator = createMockBuilder(SaslAndHMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .addMockedMethod("createSimpleAuthenticator")
            .addMockedMethod("createHMACAuthenticator")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(SaslAndHMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final JanusGraphSimpleAuthenticator janusSimpleAuthenticator = createMock(JanusGraphSimpleAuthenticator.class);
        final HMACAuthenticator hmacAuthenticator = createMock(HMACAuthenticator.class);
        final SimpleAuthenticator simpleAuthenticator = createMock(SimpleAuthenticator.class);
        final Transaction tx = createMock(Transaction.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(authenticator.createSimpleAuthenticator()).andReturn(janusSimpleAuthenticator);
        expect(authenticator.createHMACAuthenticator()).andReturn(hmacAuthenticator);
        hmacAuthenticator.setup(configMap);
        expectLastCall();
        expect(janusSimpleAuthenticator.createSimpleAuthenticator()).andReturn(simpleAuthenticator);
        simpleAuthenticator.setup(configMap);
        expectLastCall();
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(credentialGraph.findUser("user")).andReturn(createMock(Vertex.class));
        tx.rollback();
        expectLastCall();

        replayAll();

        authenticator.setup(configMap);
    }

}
