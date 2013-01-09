package com.thinkaurelius.faunus.formats.edgelist.ntriple;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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

    public void testUseFragments() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(NTripleInputFormat.USE_LOCALNAME, true);
        RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(config);
        parser.setRDFHandler(handler);

        parser.parse(new StringReader("<http://tinkerpop.com#josh> <http://tinkerpop.com#created> <http://tinkerpop.com#ripple> ."), "http://baseURI#");
        assertEquals(handler.getPredicate().getLabel(), "created");
        parser.parse(new StringReader("<http://tinkerpop.com#josh> <http://tinkerpop.com/created> <http://tinkerpop.com#ripple> ."), "http://baseURI#");
        assertEquals(handler.getPredicate().getLabel(), "created");
        parser.parse(new StringReader("<http://dbpedia.org/resource/Abraham_Lincoln> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Person> ."), "http://baseURI#");
        assertEquals(handler.getSubject().getProperty("name"), "Abraham_Lincoln");
        assertEquals(handler.getPredicate().getLabel(), "type");
        assertEquals(handler.getObject().getProperty("name"), "Person");
    }

    public void testAsProperties() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(NTripleInputFormat.USE_LOCALNAME, true);
        config.setStrings(NTripleInputFormat.AS_PROPERTIES, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
        RDFBlueprintsHandler handler = new RDFBlueprintsHandler(config);
        parser.setRDFHandler(handler);

        parser.parse(new StringReader("<http://tinkerpop.com#josh> <http://tinkerpop.com#created> <http://tinkerpop.com#ripple> ."), "http://baseURI#");
        assertEquals(handler.getPredicate().getLabel(), "created");
        parser.parse(new StringReader("<http://dbpedia.org/resource/Abraham_Lincoln> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Person> ."), "http://baseURI#");
        assertEquals(handler.getSubject().getProperty("name"), "Abraham_Lincoln");
        assertEquals(handler.getSubject().getProperty("type"), "Person");
        assertNull(handler.getPredicate());
        assertNull(handler.getObject());
    }
}
