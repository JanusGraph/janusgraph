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

import org.apache.commons.configuration.ConfigurationException;
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
    public GraphTraversalSource openGraph() throws ConfigurationException {
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
        final JanusGraphManagement mgmt = getJanusGraph().openManagement();
        try {
            // naive check if the schema was previously created
            if (mgmt.getRelationTypes(RelationType.class).iterator().hasNext()) {
                mgmt.rollback();
                return;
            }
            LOGGER.info("creating schema");
            createProperties(mgmt);
            createVertexLabels(mgmt);
            createEdgeLabels(mgmt);
            createCompositeIndexes(mgmt);
            createMixedIndexes(mgmt);
            mgmt.commit();
        } catch (Exception e) {
            mgmt.rollback();
        }
    }

    /**
     * Creates the vertex labels.
     */
    protected void createVertexLabels(final JanusGraphManagement mgmt) {
        mgmt.makeVertexLabel("titan").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();
    }

    /**
     * Creates the edge labels.
     */
    protected void createEdgeLabels(final JanusGraphManagement mgmt) {
        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("lives").signature(mgmt.getPropertyKey("reason")).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();
        mgmt.makeEdgeLabel("battled").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    protected void createProperties(final JanusGraphManagement mgmt) {
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.makePropertyKey("time").dataType(Integer.class).make();
        mgmt.makePropertyKey("reason").dataType(String.class).make();
        mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
    }

    /**
     * Creates the composite indexes. A composite index is best used for
     * exact match lookups.
     */
    protected void createCompositeIndexes(final JanusGraphManagement mgmt) {
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(mgmt.getPropertyKey("name")).buildCompositeIndex();
    }

    /**
     * Creates the mixed indexes. A mixed index requires that an external
     * indexing backend is configured on the graph instance. A mixed index
     * is best for full text search, numerical range, and geospatial queries.
     */
    protected void createMixedIndexes(final JanusGraphManagement mgmt) {
        if (useMixedIndex) {
            mgmt.buildIndex("vAge", Vertex.class).addKey(mgmt.getPropertyKey("age"))
                    .buildMixedIndex(mixedIndexConfigName);
            mgmt.buildIndex("eReasonPlace", Edge.class).addKey(mgmt.getPropertyKey("reason"))
                    .addKey(mgmt.getPropertyKey("place")).buildMixedIndex(mixedIndexConfigName);
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

        s.append("JanusGraphManagement mgmt = graph.openManagement(); ");
        s.append("boolean created = false; ");

        // naive check if the schema was previously created
        s.append(
                "if (mgmt.getRelationTypes(RelationType.class).iterator().hasNext()) { mgmt.rollback(); created = false; } else { ");

        // properties
        s.append("PropertyKey name = mgmt.makePropertyKey(\"name\").dataType(String.class).make(); ");
        s.append("PropertyKey age = mgmt.makePropertyKey(\"age\").dataType(Integer.class).make(); ");
        s.append("PropertyKey time = mgmt.makePropertyKey(\"time\").dataType(Integer.class).make(); ");
        s.append("PropertyKey reason = mgmt.makePropertyKey(\"reason\").dataType(String.class).make(); ");
        s.append("PropertyKey place = mgmt.makePropertyKey(\"place\").dataType(Geoshape.class).make(); ");

        // vertex labels
        s.append("mgmt.makeVertexLabel(\"titan\").make(); ");
        s.append("mgmt.makeVertexLabel(\"location\").make(); ");
        s.append("mgmt.makeVertexLabel(\"god\").make(); ");
        s.append("mgmt.makeVertexLabel(\"demigod\").make(); ");
        s.append("mgmt.makeVertexLabel(\"human\").make(); ");
        s.append("mgmt.makeVertexLabel(\"monster\").make(); ");

        // edge labels
        s.append("mgmt.makeEdgeLabel(\"father\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("mgmt.makeEdgeLabel(\"mother\").multiplicity(Multiplicity.MANY2ONE).make(); ");
        s.append("mgmt.makeEdgeLabel(\"lives\").signature(reason).make(); ");
        s.append("mgmt.makeEdgeLabel(\"pet\").make(); ");
        s.append("mgmt.makeEdgeLabel(\"brother\").make(); ");
        s.append("mgmt.makeEdgeLabel(\"battled\").make(); ");

        // composite indexes
        s.append("mgmt.buildIndex(\"nameIndex\", Vertex.class).addKey(name).buildCompositeIndex(); ");

        // mixed indexes
        if (useMixedIndex) {
            s.append("mgmt.buildIndex(\"vAge\", Vertex.class).addKey(age).buildMixedIndex(\"" + mixedIndexConfigName
                    + "\"); ");
            s.append("mgmt.buildIndex(\"eReasonPlace\", Edge.class).addKey(reason).addKey(place).buildMixedIndex(\""
                    + mixedIndexConfigName + "\"); ");
        }

        s.append("mgmt.commit(); created = true; }");

        return s.toString();
    }

    public static void main(String[] args) throws Exception {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final boolean drop = (args != null && args.length > 1) ? "drop".equalsIgnoreCase(args[1]) : false;
        final JanusGraphApp app = new JanusGraphApp(fileName);
        if (drop) {
            app.openGraph();
            app.dropGraph();
        } else {
            app.runApp();
        }
    }
}
