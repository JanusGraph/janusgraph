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

package org.janusgraph.example;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JanusGraphApp extends GraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(JanusGraphApp.class);

    protected static final String APP_NAME = "jgex";
    protected static final String MIXED_INDEX_CONFIG_NAME = "jgex";

    // Storage backends

    protected static final String BERKELEYJE = "berkeleyje";
    protected static final String CASSANDRA = "cassandra";
    protected static final String CQL = "cql";
    protected static final String HBASE = "hbase";
    protected static final String INMEMORY = "inmemory";

    // Index backends

    protected static final String LUCENE = "lucene";
    protected static final String ELASTICSEARCH = "elasticsearch";
    protected static final String SOLR = "solr";

    protected boolean useMixedIndex;
    protected String mixedIndexConfigName;

    /**
     * Constructs a graph app using the given properties.
     * @param fileName location of the properties file
     */
    public JanusGraphApp(final String fileName) {
        super(fileName);
        this.supportsSchema = true;
        this.supportsTransactions = true;
        this.supportsGeoshape = true;
        this.useMixedIndex = true;
        this.mixedIndexConfigName = MIXED_INDEX_CONFIG_NAME;
    }

    @Override
    public GraphTraversalSource openGraph() throws ConfigurationException, IOException {
        super.openGraph();
        useMixedIndex = useMixedIndex && conf.containsKey("index." + mixedIndexConfigName + ".backend");
        return g;
    }

    @Override
    public void dropGraph() throws Exception {
        if (graph != null) {
            JanusGraphFactory.drop(getJanusGraph());
        }
    }

    @Override
    public void createElements() {
        super.createElements();
        if (useMixedIndex) {
            try {
                // mixed indexes typically have a delayed refresh interval
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Returns the JanusGraph instance.
     */
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }

    @Override
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            // naive check if the schema was previously created
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            management.rollback();
        }
    }

    /**
     * Creates the vertex labels.
     */
    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("titan").make();
        management.makeVertexLabel("location").make();
        management.makeVertexLabel("god").make();
        management.makeVertexLabel("demigod").make();
        management.makeVertexLabel("human").make();
        management.makeVertexLabel("monster").make();
    }

    /**
     * Creates the edge labels.
     */
    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("lives").signature(management.getPropertyKey("reason")).make();
        management.makeEdgeLabel("pet").make();
        management.makeEdgeLabel("brother").make();
        management.makeEdgeLabel("battled").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).make();
        management.makePropertyKey("age").dataType(Integer.class).make();
        management.makePropertyKey("time").dataType(Integer.class).make();
        management.makePropertyKey("reason").dataType(String.class).make();
        management.makePropertyKey("place").dataType(Geoshape.class).make();
    }

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("nameIndex", Vertex.class).addKey(management.getPropertyKey("name")).buildCompositeIndex();
    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    protected void createMixedIndexes(final JanusGraphManagement management) {
        if (useMixedIndex) {
            management.buildIndex("vAge", Vertex.class).addKey(management.getPropertyKey("age"))
                    .buildMixedIndex(mixedIndexConfigName);
            management.buildIndex("eReasonPlace", Edge.class).addKey(management.getPropertyKey("reason"))
                    .addKey(management.getPropertyKey("place")).buildMixedIndex(mixedIndexConfigName);
        }
    }

    /**
     * Returns a string representation of the schema generation code. This
     * request string is submitted to the Gremlin Server via a client
     * connection to create the schema on the graph instance running on the
     * server.
     */
    protected String createSchemaRequest() {
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement management = graph.openManagement(); ");
        s.append("boolean created = false; ");

        // naive check if the schema was previously created
        s.append(
                "if (management.getRelationTypes(RelationType.class).iterator().hasNext()) { management.rollback(); created = false; } else { ");

        // properties
        s.append("PropertyKey name = management.makePropertyKey(\"name\").dataType(String.class).make(); ");
        s.append("PropertyKey age = management.makePropertyKey(\"age\").dataType(Integer.class).make(); ");
        s.append("PropertyKey time = management.makePropertyKey(\"time\").dataType(Integer.class).make(); ");
        s.append("PropertyKey reason = management.makePropertyKey(\"reason\").dataType(String.class).make(); ");
        s.append("PropertyKey place = management.makePropertyKey(\"place\").dataType(Geoshape.class).make(); ");

        // vertex labels
        s.append("management.makeVertexLabel(\"titan\").make(); ");
        s.append("management.makeVertexLabel(\"location\").make(); ");
        s.append("management.makeVertexLabel(\"god\").make(); ");
        s.append("management.makeVertexLabel(\"demigod\").make(); ");
        s.append("management.makeVertexLabel(\"human\").make(); ");
        s.append("management.makeVertexLabel(\"monster\").make(); ");

        // edge labels
        s.append("management.makeEdgeLabel(\"father\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("management.makeEdgeLabel(\"mother\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("management.makeEdgeLabel(\"lives\").signature(reason).make(); ");
        s.append("management.makeEdgeLabel(\"pet\").make(); ");
        s.append("management.makeEdgeLabel(\"brother\").make(); ");
        s.append("management.makeEdgeLabel(\"battled\").make(); ");

        // composite indexes
        s.append("management.buildIndex(\"nameIndex\", Vertex.class).addKey(name).buildCompositeIndex(); ");

        // mixed indexes
        if (useMixedIndex) {
            s.append("management.buildIndex(\"vAge\", Vertex.class).addKey(age).buildMixedIndex(\"")
                .append(mixedIndexConfigName).append("\"); ");
            s.append("management.buildIndex(\"eReasonPlace\", Edge.class).addKey(reason).addKey(place).buildMixedIndex(\"")
                .append(mixedIndexConfigName).append("\"); ");
        }

        s.append("management.commit(); created = true; }");

        return s.toString();
    }

    public static void main(String[] args) throws Exception {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final boolean drop = (args != null && args.length > 1) && "drop".equalsIgnoreCase(args[1]);
        final JanusGraphApp app = new JanusGraphApp(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
    }
}
