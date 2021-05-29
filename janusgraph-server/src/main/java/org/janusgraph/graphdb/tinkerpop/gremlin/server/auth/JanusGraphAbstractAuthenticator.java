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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

import java.net.InetAddress;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator.CONFIG_CREDENTIALS_DB;

/**
 * An abstract class to handle the initial setup of indexes for authentication.
 */
public abstract class JanusGraphAbstractAuthenticator implements Authenticator {

    private static final Logger logger = LoggerFactory.getLogger(JanusGraphAbstractAuthenticator.class);

    /**
     * Default username
     */
    public static final String CONFIG_DEFAULT_USER = "defaultUsername";

    /**
     * Default password for default username.
     */
    public static final String CONFIG_DEFAULT_PASSWORD = "defaultPassword";

    /**
     * Name of the index that enables an efficient retrievals by usernames.
     */
    public static final String USERNAME_INDEX_NAME = "byUsername";

    private CredentialTraversalSource credentials;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public abstract SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress);

    public CredentialTraversalSource createCredentials(JanusGraph graph) {
        return graph.traversal(CredentialTraversalSource.class);
    }

    public JanusGraph openGraph(String conf) {
        return JanusGraphFactory.open(conf);
    }

    @Override
    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", this.getClass().getName());
        Preconditions.checkArgument(config != null,
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    this.getClass().getName());

        Preconditions.checkState(config.containsKey(CONFIG_CREDENTIALS_DB),
            "Credential configuration missing the %s key that points to a graph config file or graph name", CONFIG_CREDENTIALS_DB);
        Preconditions.checkState(config.containsKey(CONFIG_DEFAULT_USER),
            "Credential configuration missing the %s key for the default user", CONFIG_DEFAULT_USER);
        Preconditions.checkState(config.containsKey(CONFIG_DEFAULT_PASSWORD),
            "Credential configuration missing the %s key for the default password", CONFIG_DEFAULT_PASSWORD);

        final JanusGraph graph = openGraph(config.get(CONFIG_CREDENTIALS_DB).toString());
        credentials = createCredentials(graph);
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
        if (!userExists(defaultUser)) {
            createUser(defaultUser, config.get(CONFIG_DEFAULT_PASSWORD).toString());
        }
    }

    protected Vertex findUser(final String username) {
        credentials.tx().rollback();
        final GraphTraversal<Vertex,Vertex> t = credentials.users(username);
        final Vertex v = t.hasNext() ? t.next() : null;
        if (t.hasNext()) throw new IllegalStateException(String.format("Multiple users with username %s", username));
        return v;
    }

    private boolean userExists(final String username) {
        return findUser(username) != null;
    }

    private void createUser(final String username, final String password) {
        credentials.tx().rollback();

        try {
            credentials.user(username,password).iterate();
            credentials.tx().commit();
        } catch (Exception ex) {
            credentials.tx().rollback();
            throw new RuntimeException(ex);
        }
    }
}
