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

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class RemoteGraphApp extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteGraphApp.class);

    // used for bindings
    private static final String NAME = "name";
    private static final String AGE = "age";
    private static final String TIME = "time";
    private static final String REASON = "reason";
    private static final String PLACE = "place";
    private static final String LABEL = "label";
    private static final String OUT_V = "outV";
    private static final String IN_V = "inV";

    protected JanusGraph janusgraph;
    protected Cluster cluster;
    protected Client client;
    protected Configuration conf;

    /**
     * Constructs a graph app using the given properties.
     * @param fileName location of the properties file
     */
    public RemoteGraphApp(final String fileName) {
        super(fileName);
        // the server auto-commits per request, so the application code doesn't
        // need to explicitly commit transactions
        this.supportsTransactions = false;
    }

    @Override
    public GraphTraversalSource openGraph() throws ConfigurationException, IOException {
        LOGGER.info("opening graph");
        conf = ConfigurationUtil.loadPropertiesConfig(propFileName);

        // using the remote driver for schema
        try {
            cluster = Cluster.open(conf.getString("gremlin.remote.driver.clusterFile"));
            client = cluster.connect();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }

        // using the remote graph for queries
        return traversal().withRemote(conf);
    }

    @Override
    public void createElements() {
        LOGGER.info("creating elements");

        // Use bindings to allow the Gremlin Server to cache traversals that
        // will be reused with different parameters. This minimizes the
        // number of scripts that need to be compiled and cached on the server.
        // https://tinkerpop.apache.org/docs/3.2.6/reference/#parameterized-scripts
        final Bindings b = Bindings.instance();

        // see GraphOfTheGodsFactory.java

        Vertex saturn = g.addV(b.of(LABEL, "titan")).property(NAME, b.of(NAME, "saturn"))
                .property(AGE, b.of(AGE, 10000)).next();
        Vertex sky = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sky")).next();
        Vertex sea = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "sea")).next();
        Vertex jupiter = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "jupiter")).property(AGE, b.of(AGE, 5000))
                .next();
        Vertex neptune = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "neptune")).property(AGE, b.of(AGE, 4500))
                .next();
        Vertex hercules = g.addV(b.of(LABEL, "demigod")).property(NAME, b.of(NAME, "hercules"))
                .property(AGE, b.of(AGE, 30)).next();
        Vertex alcmene = g.addV(b.of(LABEL, "human")).property(NAME, b.of(NAME, "alcmene")).property(AGE, b.of(AGE, 45))
                .next();
        Vertex pluto = g.addV(b.of(LABEL, "god")).property(NAME, b.of(NAME, "pluto")).property(AGE, b.of(AGE, 4000))
                .next();
        Vertex nemean = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "nemean")).next();
        Vertex hydra = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "hydra")).next();
        Vertex cerberus = g.addV(b.of(LABEL, "monster")).property(NAME, b.of(NAME, "cerberus")).next();
        Vertex tartarus = g.addV(b.of(LABEL, "location")).property(NAME, b.of(NAME, "tartarus")).next();

        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, saturn)).addE(b.of(LABEL, "father")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, sky)).addE(b.of(LABEL, "lives"))
                .property(REASON, b.of(REASON, "loves fresh breezes")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, neptune)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, jupiter)).as("a").V(b.of(IN_V, pluto)).addE(b.of(LABEL, "brother")).from("a").next();

        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, sea)).addE(b.of(LABEL, "lives"))
                .property(REASON, b.of(REASON, "loves waves")).from("a").next();
        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, neptune)).as("a").V(b.of(IN_V, pluto)).addE(b.of(LABEL, "brother")).from("a").next();

        g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "father")).from("a").next();
        g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, alcmene)).addE(b.of(LABEL, "mother")).from("a").next();

        if (supportsGeoshape) {
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, nemean)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 1)).property(PLACE, b.of(PLACE, Geoshape.point(38.1f, 23.7f))).from("a")
                    .next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, hydra)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 2)).property(PLACE, b.of(PLACE, Geoshape.point(37.7f, 23.9f))).from("a")
                    .next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 12)).property(PLACE, b.of(PLACE, Geoshape.point(39f, 22f))).from("a")
                    .next();
        } else {
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, nemean)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 1)).property(PLACE, b.of(PLACE, getGeoFloatArray(38.1f, 23.7f)))
                    .from("a").next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, hydra)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 2)).property(PLACE, b.of(PLACE, getGeoFloatArray(37.7f, 23.9f)))
                    .from("a").next();
            g.V(b.of(OUT_V, hercules)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "battled"))
                    .property(TIME, b.of(TIME, 12)).property(PLACE, b.of(PLACE, getGeoFloatArray(39f, 22f))).from("a")
                    .next();
        }

        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, jupiter)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, neptune)).addE(b.of(LABEL, "brother")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, tartarus)).addE(b.of(LABEL, "lives"))
                .property(REASON, b.of(REASON, "no fear of death")).from("a").next();
        g.V(b.of(OUT_V, pluto)).as("a").V(b.of(IN_V, cerberus)).addE(b.of(LABEL, "pet")).from("a").next();

        g.V(b.of(OUT_V, cerberus)).as("a").V(b.of(IN_V, tartarus)).addE(b.of(LABEL, "lives")).from("a").next();
    }

    @Override
    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
        try {
            if (g != null) {
                // this closes the remote, no need to close the empty graph
                g.close();
            }
            if (cluster != null) {
                // the cluster closes all of its clients
                cluster.close();
            }
        } finally {
            g = null;
            graph = null;
            client = null;
            cluster = null;
        }
    }

    @Override
    public void createSchema() {
        LOGGER.info("creating schema");
        // get the schema request as a string
        final String req = createSchemaRequest();
        // submit the request to the server
        final ResultSet resultSet = client.submit(req);
        // drain the results completely
        Stream<Result> futureList = resultSet.stream();
        futureList.map(Result::toString).forEach(LOGGER::info);
    }

    public static void main(String[] args) {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final RemoteGraphApp app = new RemoteGraphApp(fileName);
        app.runApp();
    }
}
