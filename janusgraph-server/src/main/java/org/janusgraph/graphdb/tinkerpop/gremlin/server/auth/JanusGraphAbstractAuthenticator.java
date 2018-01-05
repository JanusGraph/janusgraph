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

import java.net.InetAddress;
import java.util.Map;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraph;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An abstract class to handle the initial setup of indexes for authentication.
 */
abstract public class JanusGraphAbstractAuthenticator implements Authenticator {

    private static final Logger logger = LoggerFactory.getLogger(JanusGraphAbstractAuthenticator.class);

    /**
     * Default username
     */
    public static final String CONFIG_DEFAULT_USER = "defaultUsername";

    /**
     * Default password for default username.
     */
    public static final String CONFIG_DEFAULT_PASSWORD = "defaultPassword";

    private static final String USERNAME_INDEX_NAME = "byUsername";

    protected CredentialGraph credentialStore;
    protected JanusGraph backingGraph;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    abstract public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress);

    public CredentialGraph createCredentialGraph(JanusGraph graph) {
        return new CredentialGraph(graph);
    }

    public JanusGraph openGraph(String conf) {
        return JanusGraphFactory.open(conf);
    }

    @Override
    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", this.getClass().getName());
        Preconditions.checkArgument(config != null, String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    this.getClass().getName()));
        

        Preconditions.checkState(config.containsKey(CONFIG_CREDENTIALS_DB), String.format(
                    "Credential configuration missing the %s key that points to a graph config file or graph name", CONFIG_CREDENTIALS_DB));

        if (!config.containsKey(CONFIG_DEFAULT_USER) || !config.containsKey(CONFIG_DEFAULT_PASSWORD)) {
            logger.warn(String.format("%s and %s should be defined to bootstrap authentication", CONFIG_DEFAULT_USER, CONFIG_DEFAULT_PASSWORD));
        }

        final JanusGraph graph = openGraph(config.get(CONFIG_CREDENTIALS_DB).toString());
        backingGraph = graph;
        credentialStore = createCredentialGraph(graph);
        graph.tx().rollback();
        ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        if (!mgmt.containsGraphIndex(USERNAME_INDEX_NAME)) {
            final PropertyKey username = mgmt.makePropertyKey(PROPERTY_USERNAME).dataType(String.class).cardinality(Cardinality.SINGLE).make();
            mgmt.buildIndex(USERNAME_INDEX_NAME, Vertex.class).addKey(username).unique().buildCompositeIndex();
            mgmt.commit();
            mgmt = (ManagementSystem) graph.openManagement();
            final JanusGraphIndex index = mgmt.getGraphIndex(USERNAME_INDEX_NAME);
            if (!index.getIndexStatus(username).equals(SchemaStatus.ENABLED)) {
                try {
                    mgmt = (ManagementSystem) graph.openManagement();
                    mgmt.updateIndex(mgmt.getGraphIndex(USERNAME_INDEX_NAME), SchemaAction.REINDEX);
                    ManagementSystem.awaitGraphIndexStatus(graph, USERNAME_INDEX_NAME).status(SchemaStatus.ENABLED).call();
                    mgmt.commit();
                } catch (InterruptedException rude) {
                    mgmt.rollback();
                    throw new RuntimeException("Timed out waiting for byUsername index to be created on credential graph", rude);
                }
            }
        }

        final String defaultUser = config.get(CONFIG_DEFAULT_USER).toString();
        if (credentialStore.findUser(defaultUser) == null) {
            credentialStore.createUser(defaultUser, config.get(CONFIG_DEFAULT_PASSWORD).toString());
        }
    }
}
