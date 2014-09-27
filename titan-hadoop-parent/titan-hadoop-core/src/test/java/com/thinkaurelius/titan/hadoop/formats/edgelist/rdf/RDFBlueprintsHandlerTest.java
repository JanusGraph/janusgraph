package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.thinkaurelius.titan.hadoop.FaunusSchemaManager;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.FaunusElement;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.thinkaurelius.titan.hadoop.formats.edgelist.rdf.RDFConfig.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFBlueprintsHandlerTest extends TestCase {

    public void testMD5HashUniqueness() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        final Set<Long> ids = new HashSet<Long>();
        int loops = 1000000;
        for (int i = 0; i < loops; i++) {
            ids.add(ByteBuffer.wrap(md.digest(("http://test#" + UUID.randomUUID().toString()).getBytes())).getLong());
        }
        assertEquals(ids.size(), loops);
    }

    public void testCrc64HashUniqueness() throws NoSuchAlgorithmException {
        final Set<Long> ids = new HashSet<Long>();
        int loops = 1000000;
        for (int i = 0; i < loops; i++) {
            ids.add(Crc64.digest(("http://test#" + UUID.randomUUID().toString()).getBytes()));
        }
        assertEquals(ids.size(), loops);
    }

    public void testUseFragments() throws Exception {
        FaunusSchemaManager.getTypeManager(null).clear();
        ModifiableHadoopConfiguration faunusConf = new ModifiableHadoopConfiguration();
        faunusConf.getInputConf(ROOT_NS).set(RDF_USE_LOCALNAME, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_FORMAT, Syntax.N_TRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(faunusConf);

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#created> <http://tinkerpop.com#ripple> .");
        handler.next();
        handler.next();
        assertEquals(((StandardFaunusEdge) handler.next()).getLabel(), "created");

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com/created> <http://tinkerpop.com#ripple> .");
        handler.next();
        handler.next();
        assertEquals(((StandardFaunusEdge) handler.next()).getLabel(), "created");

        handler.parse("<http://dbpedia.org/resource/Abraham_Lincoln> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Person> .");
        assertEquals(handler.next().getProperty("name"), "Abraham_Lincoln");
        assertEquals(handler.next().getProperty("name"), "Person");
        FaunusElement faunusElement = handler.next();
        assertNotNull(faunusElement);
        assertTrue(faunusElement instanceof StandardFaunusEdge);
        assertEquals("type", ((StandardFaunusEdge)faunusElement).getLabel());
        assertFalse(handler.hasNext());

        handler.parse("<http://dbpedia.org/resource/Abraham_Lincoln> <http://www.w3.org/2000/01/rdf-schema#label> \"Abraham Lincoln\" .");
        FaunusVertex abe = (FaunusVertex) handler.next();
        assertEquals(abe.getProperty("name"), "Abraham_Lincoln");
        assertEquals(handler.next().getProperty("name"), "Abraham Lincoln");
        // note "label_", not the reserved "label"
        assertEquals(((StandardFaunusEdge) handler.next()).getLabel(), "label_");
        assertFalse(handler.hasNext());
    }

    public void testAsProperties() throws Exception {
        FaunusSchemaManager.getTypeManager(null).clear();
        ModifiableHadoopConfiguration faunusConf = new ModifiableHadoopConfiguration();
        faunusConf.getInputConf(ROOT_NS).set(RDF_USE_LOCALNAME, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_AS_PROPERTIES, new String[] { "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" });
        faunusConf.getInputConf(ROOT_NS).set(RDF_FORMAT, Syntax.N_TRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(faunusConf);

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#created> <http://tinkerpop.com#ripple> .");
        assertTrue(handler.hasNext());
        assertTrue(handler.hasNext());
        handler.next();
        assertTrue(handler.hasNext());
        handler.next();
        assertTrue(handler.hasNext());
        assertEquals(((StandardFaunusEdge) handler.next()).getLabel(), "created");
        assertFalse(handler.hasNext());
        handler.parse("<http://dbpedia.org/resource/Abraham_Lincoln> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Person> .");
        FaunusVertex subject = (FaunusVertex) handler.next();
        assertEquals(subject.getProperty("name"), "Abraham_Lincoln");
        assertEquals(subject.getProperty("type"), "Person");
        assertFalse(handler.hasNext());
    }


    public void testLiteralProperties() throws Exception {
        FaunusSchemaManager.getTypeManager(null).clear();
        ModifiableHadoopConfiguration faunusConf = new ModifiableHadoopConfiguration();
        faunusConf.getInputConf(ROOT_NS).set(RDF_USE_LOCALNAME, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_LITERAL_AS_PROPERTY, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_FORMAT, Syntax.N_TRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(faunusConf);

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#age> \"32\"^^<http://www.w3.org/2001/XMLSchema#int> .");
        FaunusElement subject = handler.next();
        assertEquals(subject.getProperty("name"), "josh");
        assertEquals(subject.getProperty("age"), 32);
        assertFalse(handler.hasNext());

        handler.parse("<http://tinkerpop.com#marko> <http://tinkerpop.com#firstname> \"marko\"^^<http://www.w3.org/2001/XMLSchema#string> .");
        subject = handler.next();
        assertEquals(subject.getProperty("name"), "marko");
        assertEquals(subject.getProperty("firstname"), "marko");
        assertFalse(handler.hasNext());

        handler.parse("<http://tinkerpop.com#stephen> <http://tinkerpop.com#location> \"1.023\"^^<http://www.w3.org/2001/XMLSchema#double> .");
        subject = handler.next();
        assertEquals(subject.getProperty("name"), "stephen");
        assertEquals(subject.getProperty("location"), 1.023d);
        assertFalse(handler.hasNext());

        handler.parse("<http://tinkerpop.com#stephen> <http://tinkerpop.com#alive> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .");
        subject = handler.next();
        assertEquals(subject.getProperty("name"), "stephen");
        assertEquals(subject.getProperty("alive"), true);
        assertFalse(handler.hasNext());

        handler.parse("<http://tinkerpop.com#stephen> <http://tinkerpop.com#ttl> \"1234567890005543\"^^<http://www.w3.org/2001/XMLSchema#long> .");
        subject = handler.next();
        assertEquals(subject.getProperty("name"), "stephen");
        assertEquals(subject.getProperty("ttl"), 1234567890005543l);
        assertFalse(handler.hasNext());

        handler.parse("<http://tinkerpop.com#stephen> <http://tinkerpop.com#height> \"0.45\"^^<http://www.w3.org/2001/XMLSchema#float> .");
        subject = handler.next();
        assertEquals(subject.getProperty("name"), "stephen");
        assertEquals(subject.getProperty("height"), 0.45f);
        assertFalse(handler.hasNext());

    }

    public void testMultiLineParse() throws Exception {
        FaunusSchemaManager.getTypeManager(null).clear();
        ModifiableHadoopConfiguration faunusConf = new ModifiableHadoopConfiguration();
        faunusConf.getInputConf(ROOT_NS).set(RDF_USE_LOCALNAME, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_LITERAL_AS_PROPERTY, true);
        faunusConf.getInputConf(ROOT_NS).set(RDF_FORMAT, Syntax.N_TRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(faunusConf);

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#age> \"32\"^^<http://www.w3.org/2001/XMLSchema#int> .");
        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#knows> <http://tinkerpop.com#marko> .");

        FaunusVertex josh = (FaunusVertex) handler.next();
        assertEquals(josh.getProperty("age"), 32);
        assertEquals(josh.getProperty("name"), "josh");
        assertEquals(josh.getPropertyKeys().size(), 3);
        josh = (FaunusVertex) handler.next();
        assertEquals(josh.getProperty("name"), "josh");
        assertEquals(josh.getPropertyKeys().size(), 2);
        FaunusVertex marko = (FaunusVertex) handler.next();
        assertEquals(marko.getProperty("name"), "marko");
        assertEquals(marko.getPropertyKeys().size(), 2);
        StandardFaunusEdge knows = (StandardFaunusEdge) handler.next();
        assertEquals(knows.getLabel(), "knows");
        assertEquals(knows.getPropertyKeys().size(), 1);
        assertFalse(handler.hasNext());
    }

    /*
    TODO: Make multiline work with buffering
    public void testMultiLineTriple() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(RDFInputFormat.TITAN_HADOOP_GRAPH_INPUT_RDF_USE_LOCALNAME, true);
        config.setBoolean(RDFInputFormat.TITAN_HADOOP_GRAPH_INPUT_RDF_LITERAL_AS_PROPERTY, true);
        config.set(RDFInputFormat.TITAN_HADOOP_GRAPH_INPUT_RDF_FORMAT, "n-triples");
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(config);

        handler.parse("<http://tinkerpop.com#josh> <http://tinkerpop.com#age> ");
        handler.parse("\"32\"^^<http://www.w3.org/2001/XMLSchema#int> .");

        HadoopVertex josh = (HadoopVertex) handler.next();
        assertEquals(josh.getProperty("age"), 32);
        assertEquals(josh.getProperty("name"), "josh");
        assertEquals(josh.getPropertyKeys().size(), 3);
        assertFalse(handler.hasNext());
    }*/
}
