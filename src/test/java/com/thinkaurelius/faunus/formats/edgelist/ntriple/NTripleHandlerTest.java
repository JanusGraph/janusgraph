package com.thinkaurelius.faunus.formats.edgelist.ntriple;

import junit.framework.TestCase;
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
public class NTripleHandlerTest extends TestCase {

    public void testMD5HashUniqueness() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        final Set<Long> ids = new HashSet<Long>();
        int loops = 1000000;
        for (int i = 0; i < loops; i++) {
            ids.add(ByteBuffer.wrap(md.digest(("http://test#" + UUID.randomUUID().toString()).getBytes())).getLong());
        }
        assertEquals(ids.size(), loops);

    }

    public void testURIConversions() throws Exception {
        RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
        NTripleHandler handler = new NTripleHandler(false);
        parser.setRDFHandler(handler);
        parser.parse(new StringReader("<http://tinkerpop.com#josh> <http://tinkerpop.com#created> <http://tinkerpop.com#ripple> ."), "http://baseURI#");
        assertEquals(handler.getPredicate().getLabel(), "created");
    }
}
