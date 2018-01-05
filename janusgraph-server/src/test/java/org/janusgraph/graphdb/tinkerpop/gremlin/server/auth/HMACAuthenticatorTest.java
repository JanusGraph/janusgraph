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

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator.CONFIG_CREDENTIALS_DB;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_TOKEN;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

import java.net.InetAddress;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraph;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
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
import org.mindrot.jbcrypt.BCrypt;

public class HMACAuthenticatorTest extends EasyMockSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testSetupNullConfig() {
        final HMACAuthenticator authenticator = new HMACAuthenticator();
        authenticator.setup(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupNoCredDb() {
        final HMACAuthenticator authenticator = new HMACAuthenticator();
        authenticator.setup(new HashMap<String, Object>());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupNoHmacSecret() {
        final HMACAuthenticator authenticator = new HMACAuthenticator();
        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        authenticator.setup(configMap);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupEmptyNoUserDefault() {
        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        authenticator.setup(configMap);
    }

    @Test(expected=IllegalStateException.class)
    public void testSetupEmptyCredGraphNoPassDefault() {
        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        authenticator.setup(configMap);
    }

    @Test
    public void testSetupEmptyCredGraphNoUserIndex() {
        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final PropertyKey pk = createMock(PropertyKey.class);
        final PropertyKeyMaker pkm = createMock(PropertyKeyMaker.class);
        final JanusGraphManagement.IndexBuilder indexBuilder = createMock(JanusGraphManagement.IndexBuilder.class);
        final JanusGraphIndex index = createMock(JanusGraphIndex.class);
        final PropertyKey[] pks = {pk};

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
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
        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
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
        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(credentialGraph.findUser("user")).andReturn(createMock(Vertex.class));
        tx.rollback();
        expectLastCall();

        replayAll();

        authenticator.setup(configMap);
    }

    @Test
    public void testAuthenticateBasicAuthValid() throws AuthenticationException {
        final Map<String, String> credentials = new HashMap<>();
        credentials.put(PROPERTY_USERNAME, "user");
        credentials.put(PROPERTY_PASSWORD, "pass");

        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);
        final String bcryptedPass = BCrypt.hashpw("pass", BCrypt.gensalt(4));

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).times(2);
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        authenticator.authenticate(credentials);
        verifyAll();
    }

    @Test(expected=AuthenticationException.class)
    public void testAuthenticateBasicAuthInvalid() throws AuthenticationException {
       final Map<String, String> credentials = new HashMap<>();
       credentials.put(PROPERTY_USERNAME, "user");
       credentials.put(PROPERTY_PASSWORD, "invalid");

       final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);
        final String bcryptedPass = BCrypt.hashpw("pass", BCrypt.gensalt(4));

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).times(2);
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass);
        expect(graph.tx()).andReturn(tx);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        authenticator.authenticate(credentials);
        verifyAll();

    }

    @Test
    public void testTokenAuth() throws AuthenticationException {
        final Map<String, String> sharedVars = testAuthenticateGenerateToken();
        testAuthenticateWithToken(sharedVars);
        testFailureShortenedToken(sharedVars);
        testTokenTimeout(sharedVars);
    }

    private Map<String, String> testAuthenticateGenerateToken() throws AuthenticationException {
        final Map<String, String> credentials = new HashMap<>();
        credentials.put(PROPERTY_USERNAME, "user");
        credentials.put(PROPERTY_PASSWORD, "pass");
        credentials.put(PROPERTY_GENERATE_TOKEN, "true");

        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_ALGO, "HmacSHA256");

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);
        final String bcryptedPass = BCrypt.hashpw("pass", BCrypt.gensalt(4));

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).anyTimes();
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass).times(2);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        assertNotNull(authenticator.authenticate(credentials));
        final String hmacToken = credentials.get(PROPERTY_TOKEN);
        assertNotNull(hmacToken);
        verifyAll();
        resetAll();
        final Map<String, String> sharedVars = new HashMap<>();
        sharedVars.put("hmacToken", hmacToken);
        sharedVars.put("encryptedPass", bcryptedPass);
        return sharedVars;
    }

    private void testFailureShortenedToken(final Map<String, String> sharedVars) throws AuthenticationException {
        final String token = sharedVars.get("hmacToken");
        final String bcryptedPass = sharedVars.get("encryptedPass");
        final Map<String, String> credentials = new HashMap<>();
        final String encodedString = new String(Base64.getUrlDecoder().decode(token));
        final String brokenToken = encodedString.substring(0, encodedString.length() - 5);
        credentials.put(PROPERTY_TOKEN, brokenToken);

        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_ALGO, "HmacSHA256");
        configMap.put(HMACAuthenticator.CONFIG_TOKEN_TIMEOUT, 3600000);

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).anyTimes();
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        try {
            authenticator.authenticate(credentials);
            assertFalse(true);
        } catch (AuthenticationException ex) {
            assertNotNull(ex);
            verifyAll();
            resetAll();
        }
    }

    private void testAuthenticateWithToken(final Map<String, String> sharedVars) throws AuthenticationException {
        final String token = sharedVars.get("hmacToken");
        final String bcryptedPass = sharedVars.get("encryptedPass");
        final Map<String, String> credentials = new HashMap<>();
        credentials.put(PROPERTY_TOKEN, new String(Base64.getUrlDecoder().decode(token)));

        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_ALGO, "HmacSHA256");
        configMap.put(HMACAuthenticator.CONFIG_TOKEN_TIMEOUT, 3600000);

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).anyTimes();
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        assertNotNull(authenticator.authenticate(credentials));
        verifyAll();
        resetAll();
    }

    private void testTokenTimeout(final Map<String, String> sharedVars) {
        final String token = sharedVars.get("hmacToken");
        final String bcryptedPass = sharedVars.get("encryptedPass");
        final Map<String, String> credentials = new HashMap<>();
        credentials.put(PROPERTY_TOKEN, new String(Base64.getUrlDecoder().decode(token)));

        final HMACAuthenticator authenticator = createMockBuilder(HMACAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        final Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(CONFIG_CREDENTIALS_DB, "configCredDb");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_SECRET, "secret");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(HMACAuthenticator.CONFIG_DEFAULT_USER, "user");
        configMap.put(HMACAuthenticator.CONFIG_HMAC_ALGO, "HmacSHA256");
        configMap.put(HMACAuthenticator.CONFIG_TOKEN_TIMEOUT, 1);

        final JanusGraph graph = createMock(JanusGraph.class);
        final CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        final ManagementSystem mgmt = createMock(ManagementSystem.class);
        final Transaction tx = createMock(Transaction.class);
        final Vertex userVertex = createMock(Vertex.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.findUser(eq("user"))).andReturn(userVertex).anyTimes();
        expect(userVertex.value(eq(PROPERTY_PASSWORD))).andReturn(bcryptedPass);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        tx.rollback();
        expectLastCall();

        replayAll();
        authenticator.setup(configMap);
        AuthenticationException ae = null;
        try {
            authenticator.authenticate(credentials);
        } catch (AuthenticationException e) {
            ae = e;
        }
        assertNotNull(ae);
        verifyAll();

    }

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiatorOfInetAddr() {
        final HMACAuthenticator authenticator = new HMACAuthenticator();
        authenticator.newSaslNegotiator(createMock(InetAddress.class));
    }

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiator() {
        final HMACAuthenticator authenticator = new HMACAuthenticator();
        authenticator.newSaslNegotiator();
    }
}
