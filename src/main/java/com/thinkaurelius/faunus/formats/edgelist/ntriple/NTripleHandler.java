package com.thinkaurelius.faunus.formats.edgelist.ntriple;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NTripleHandler implements RDFHandler {

    private final MessageDigest md;
    private final FaunusVertex subject = new FaunusVertex();
    private final FaunusVertex object = new FaunusVertex();
    private final FaunusEdge predicate = new FaunusEdge();
    private final boolean enablePath;

    public NTripleHandler(final boolean enablePath) throws IOException {
        this.enablePath = enablePath;
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public void startRDF() throws RDFHandlerException {
        // Do nothing
    }

    public void endRDF() throws RDFHandlerException {
        // Do nothing
    }

    public void handleNamespace(String s, String s1) throws RDFHandlerException {
        // Do nothing
    }

    public void handleStatement(final Statement s) throws RDFHandlerException {
        ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
        long subjectId = bb.getLong();
        this.subject.reuse(subjectId);
        this.subject.setProperty(NTripleInputFormat.URI, s.getSubject().stringValue());
        this.subject.enablePath(this.enablePath);

        bb = ByteBuffer.wrap(md.digest(s.getObject().stringValue().getBytes()));
        long objectId = bb.getLong();
        this.object.reuse(objectId);
        this.object.setProperty(NTripleInputFormat.URI, s.getObject().stringValue());
        this.object.enablePath(this.enablePath);

        this.predicate.reuse(-1, subjectId, objectId, s.getPredicate().getLocalName());
        this.predicate.setProperty(NTripleInputFormat.URI, s.getPredicate().stringValue());
        if (null != s.getContext())
            this.predicate.setProperty(NTripleInputFormat.CONTEXT, s.getContext().stringValue());
        this.predicate.enablePath(this.enablePath);
    }

    public void handleComment(String s) throws RDFHandlerException {
        // Do nothing
    }

    public FaunusVertex getSubject() {
        return this.subject;
    }

    public FaunusVertex getObject() {
        return this.object;
    }

    public FaunusEdge getPredicate() {
        return this.predicate;
    }
}
