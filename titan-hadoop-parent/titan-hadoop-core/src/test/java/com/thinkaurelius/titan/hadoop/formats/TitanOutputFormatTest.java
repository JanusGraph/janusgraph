package com.thinkaurelius.titan.hadoop.formats;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.schema.SchemaContainer;
import com.thinkaurelius.titan.graphdb.schema.SchemaProvider;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Imports;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.PipeHelper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.junit.Assert.*;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanOutputFormatTest extends BaseTestNG {

    /**
     * Return a TitanGraph configuration for the particular storage backend under test.
     * For instance, a subclass testing the Cassandra input format would return a
     * configuration with storage.backend set to cassandrathrift/cassandra, etc.
     */
    protected abstract ModifiableConfiguration getTitanConfiguration();

    /**
     * Return the full package.classname of the InputFormat to use when reading from Titan
     */
    protected abstract Class<?> getTitanInputFormatClass();

    /**
     * Return the full package.classname of the OutputFormat to use when writing to Titan
     */
    protected abstract Class<?> getTitanOutputFormatClass();

    /**
     * Set any additional Faunus/Hadoop options on the config about to used to
     * instantiate a HadoopGraph instance.  Subclasses can use this method to set or
     * overwrite arbitrary config keys right before HadoopGraph construction.
     */
    protected void setCustomFaunusOptions(ModifiableHadoopConfiguration c) { /* default noop */ }

    private TitanGraph g;

    private HadoopGraph f1, f2;

    @After
    public void cleanUp() {
        close();
    }

    public void clear() throws Exception {
        ModifiableConfiguration mc = getTitanConfiguration();
        mc.set(UNIQUE_INSTANCE_ID, "deleter");
        mc.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
        Backend backend = new Backend(mc);
        backend.initialize(mc);
        backend.clearStorage();
        backend.close();
    }

    @Before
    public void setUp() throws Exception {
        clear();
        open();
        f1 = getGraphSONToTitan();
        f2 = getTitanToTitan();
    }

    @Test
    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(getTitanOutputFormatClass().getPackage().getName() + ".*"));
    }

    @Test
    public void testBulkLoading() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        new GremlinPipeline(g).V().sideEffect(new PipeFunction<Vertex, Vertex>() {
            @Override
            public Vertex compute(Vertex vertex) {
                assertEquals(2, vertex.getPropertyKeys().size());
                assertNotNull(vertex.getProperty("name"));
                return vertex;
            }
        }).iterate();
        assertEquals("saturn", new GremlinPipeline(g).V("name", "hercules").out("father").out("father").property("name").next());
        List names = new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).inV().property("name").toList();
        assertTrue(names.contains("nemean"));
        assertTrue(names.contains("hydra"));
        assertTrue(names.contains("cerberus"));
    }

    @Test
    public void testBulkElementDeletions() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        new HadoopPipeline(f2).V().drop().submit();
        clopen();
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).E().drop().submit();
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
        new HadoopPipeline(f2).V().drop().submit();
        clopen();
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());
    }

    @Test
    public void testFewElementDeletions() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        new HadoopPipeline(f2).E().has("label", "battled").drop().submit();
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(14, new GremlinPipeline(g).E().count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "battled").count());
        assertEquals(1, new GremlinPipeline(g).E().has("label", "mother").count());
        assertEquals(2, new GremlinPipeline(g).E().has("label", "father").count());

        new HadoopPipeline(f2).V().has("name", "hercules").drop().submit();
        clopen();
        assertEquals(11, new GremlinPipeline(g).V().count());
        assertEquals(12, new GremlinPipeline(g).E().count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "battled").count());
        assertEquals(0, new GremlinPipeline(g).E().has("label", "mother").count());
        assertEquals(1, new GremlinPipeline(g).E().has("label", "father").count());
    }

    @Test
    public void testBulkVertexPropertyDeletions() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).V().sideEffect("{it.removeProperty('name')}").submit();
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertNull(v.getProperty("name"));
            assertEquals(1, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();
    }

    @Test
    public void testBulkVertexPropertyUpdates() throws Exception {
        // Declare schema in Titan
        TitanManagement mgmt = g.getManagementSystem();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.commit();

        // Reload schema from Titan into Faunus's Type Manager
        FaunusTypeManager.getTypeManager(null).clear();
        SchemaProvider titanSchemaProvider = new SchemaContainer(g);
        FaunusTypeManager typeManager = FaunusTypeManager.getTypeManager(null); //argument is ignored
        typeManager.setSchemaProvider(titanSchemaProvider);

        bulkLoadGraphOfTheGods(f1);

        new HadoopPipeline(f2).V().sideEffect("{it.name = 'marko' + it.name}").submit();

        clopen();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        for (Vertex v : g.getVertices()) {
            assertTrue(v.<List<String>>getProperty("name").get(0).startsWith("marko"));
            assertEquals(v.<List<String>>getProperty("name").size(), 1);
            assertEquals(2, v.getPropertyKeys().size());
        }
        new GremlinPipeline(g).V("name", "hercules").outE("battled").sideEffect(new PipeFunction<Edge, Edge>() {
            @Override
            public Edge compute(Edge edge) {
                assertNotNull(edge.getProperty("time"));
                return edge;
            }
        }).iterate();

        new HadoopPipeline(f2).V().drop().submit();
        clopen();
        assertEquals(0, new GremlinPipeline(g).V().count());
        assertEquals(0, new GremlinPipeline(g).E().count());

        bulkLoadGraphOfTheGods(f1);

        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        int counter = 0;
        clopen();
        new HadoopPipeline(f2).V().has("name", "saturn").sideEffect("{it.addProperty('name','chronos')}").submit();

        TitanVertex v = (TitanVertex) g.getVertices("name", "saturn").iterator().next();
        for (Object property : new GremlinPipeline(v).transform(new PipeFunction<TitanVertex, Iterable<TitanProperty>>() {
            @Override
            public Iterable<TitanProperty> compute(TitanVertex vertex) {
                return vertex.getProperties("name");
            }
        }).scatter().toList()) {
            String value = (String) ((TitanProperty) property).getValue();
            assertTrue(value.equals("saturn") || value.equals("chronos"));
            counter++;
        }
        assertEquals(counter, 2);

        // Reset/clear types to avoid interference with subsequent tests
        typeManager.clear();
        typeManager.setSchemaProvider(DefaultSchemaProvider.INSTANCE);
    }

    @Test
    public void testBulkEdgeDerivations() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).V().as("x").out("father").out("father").linkIn("grandfather", "x").submit();

        clopen();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(18, new GremlinPipeline(g).E().count());

        assertTrue(PipeHelper.areEqual(
                new GremlinPipeline(g).V("name", "hercules").out("father").out("father"),
                new GremlinPipeline(g).V("name", "hercules").out("grandfather")));
    }

    @Test
    public void testBulkEdgePropertyUpdates() throws Exception {
        bulkLoadGraphOfTheGods(f1);
        new HadoopPipeline(f2).E().has("label", "battled").sideEffect("{it.time = it.time+1}").submit();

        clopen();

        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());

        int counter = 0;
        for (Edge edge : g.getEdges()) {
            if (edge.getLabel().equals("battled")) {
                assertEquals(edge.getPropertyKeys().size(), 1);
                int time = edge.getProperty("time");
                assertTrue(time == 2 || time == 3 || time == 13);
                counter++;
            } else {
                assertEquals(edge.getPropertyKeys().size(), 0);
            }
        }
        assertEquals(counter, 3);
        assertEquals(3, new GremlinPipeline(g).V("name", "hercules").outE("battled").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").outE("father").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").outE("mother").count());
        assertEquals(3, new GremlinPipeline(g).V("name", "hercules").out("battled").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").out("father").count());
        assertEquals(1, new GremlinPipeline(g).V("name", "hercules").out("mother").count());
        assertEquals(5, new GremlinPipeline(g).V("name", "hercules").out().count());
    }

    @Test
    public void testUnidirectionEdges() throws Exception {
        // Declare schema in Titan
        TitanManagement mgmt = g.getManagementSystem();
        mgmt.makeEdgeLabel("father").unidirected().make();
        mgmt.commit();

//        // Reload schema from Titan into Faunus's Type Manager
//        FaunusTypeManager.getTypeManager(null).clear();
//        SchemaProvider titanSchemaProvider = new SchemaContainer(g);
//        FaunusTypeManager typeManager = FaunusTypeManager.getTypeManager(null); //argument is ignored
//        typeManager.setSchemaProvider(titanSchemaProvider);

        bulkLoadGraphOfTheGods(f1);
        clopen();
        assertEquals(12, new GremlinPipeline(g).V().count());
        assertEquals(17, new GremlinPipeline(g).E().count());
        assertEquals(new GremlinPipeline(g).V("name", "hercules").out("father").count(), 1);
        assertEquals(new GremlinPipeline(g).V("name", "jupiter").in("father").count(), 0);

//        // Reset/clear types to avoid interference with subsequent tests
//        typeManager.clear();
//        typeManager.setSchemaProvider(DefaultSchemaProvider.INSTANCE);
    }

    // TODO: Unidirectional edges test cases
    // TODO: Multi-properties

    private void close() {
        if (null != g && g.isOpen())
            g.shutdown();
        g = null;
    }

    private void open() {
        g = TitanFactory.open(getTitanConfiguration());
    }

    protected void clopen() {
        close();
        open();
    }

    private void bulkLoadGraphOfTheGods(final HadoopGraph f) throws Exception {
        new HadoopPipeline(f)._().submit();
    }

    private HadoopGraph getGraphSONToTitan() {
        ModifiableHadoopConfiguration faunusConf =
                new ModifiableHadoopConfiguration();

        // Input
        faunusConf.set(INPUT_FORMAT, GraphSONInputFormat.class.getCanonicalName());
        faunusConf.set(INPUT_LOCATION, "target/test-classes/com/thinkaurelius/titan/hadoop/formats/graphson/graph-of-the-gods.json");

        // Output
        ModifiableConfiguration titanConf = getTitanConfiguration();
        faunusConf.set(OUTPUT_FORMAT, getTitanOutputFormatClass().getCanonicalName());
        faunusConf.setAllOutput(titanConf.getAll());

        setCommonFaunusOptions(faunusConf);
        setCustomFaunusOptions(faunusConf);

        return new HadoopGraph(faunusConf.getHadoopConfiguration());
    }

    private HadoopGraph getTitanToTitan() {
        ModifiableHadoopConfiguration faunusConf =
                new ModifiableHadoopConfiguration();

        ModifiableConfiguration titanConf = getTitanConfiguration();

        // Input
        faunusConf.set(INPUT_FORMAT, getTitanInputFormatClass().getCanonicalName());
        faunusConf.setAllInput(titanConf.getAll());

        // Output
        faunusConf.set(OUTPUT_FORMAT, getTitanOutputFormatClass().getCanonicalName());
        faunusConf.setAllOutput(titanConf.getAll());

        setCommonFaunusOptions(faunusConf);
        setCustomFaunusOptions(faunusConf);

        return new HadoopGraph(faunusConf.getHadoopConfiguration());
    }

    private void setCommonFaunusOptions(ModifiableHadoopConfiguration faunusConf) {
        // Side effect and misc
        faunusConf.set(OUTPUT_INFER_SCHEMA, true);
        faunusConf.set(PIPELINE_TRACK_PATHS, true);
        faunusConf.set(PIPELINE_TRACK_STATE, true);
        faunusConf.set(SIDE_EFFECT_FORMAT, TextOutputFormat.class.getCanonicalName());
        faunusConf.set(JOBDIR_LOCATION, Joiner.on(File.separator).join("target", "test-data", "output"));
        faunusConf.set(JOBDIR_OVERWRITE, true);
    }
}
