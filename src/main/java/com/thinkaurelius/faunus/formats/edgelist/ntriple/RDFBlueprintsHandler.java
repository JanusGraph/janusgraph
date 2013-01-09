package com.thinkaurelius.faunus.formats.edgelist.ntriple;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFBlueprintsHandler implements RDFHandler {

    private final MessageDigest md;
    private FaunusVertex subject = new FaunusVertex();
    private FaunusVertex object = new FaunusVertex();
    private FaunusEdge predicate = new FaunusEdge();
    private final boolean enablePath;
    private final boolean useFragments;
    private final Set<String> asProperties = new HashSet<String>();


    public RDFBlueprintsHandler(final Configuration configuration) throws IOException {
        this.enablePath = configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false);
        this.useFragments = configuration.getBoolean(NTripleInputFormat.USE_LOCALNAME, false);
        for (final String property : configuration.getStringCollection(NTripleInputFormat.AS_PROPERTIES)) {
            this.asProperties.add(property);
        }

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

    public String postProcess(final Value resource) {
        if (resource instanceof URI) {
            if (this.useFragments) {
                return ((URI) resource).getLocalName();
            } else {
                return resource.stringValue();
            }
        } else {
            return resource.stringValue();
        }
    }

    public void handleStatement(final Statement s) throws RDFHandlerException {
        if (this.asProperties.contains(s.getPredicate().toString())) {
            final ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
            this.subject.reuse(bb.getLong());
            this.subject.setProperty(postProcess(s.getPredicate()), postProcess(s.getObject()));
            this.subject.setProperty(NTripleInputFormat.URI, s.getSubject().stringValue());
            if (this.useFragments)
                this.subject.setProperty(NTripleInputFormat.NAME, postProcess(s.getSubject()));
            this.subject.enablePath(this.enablePath);
            this.object = null;
            this.predicate = null;
        } else {
            ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
            long subjectId = bb.getLong();
            this.subject.reuse(subjectId);
            this.subject.setProperty(NTripleInputFormat.URI, s.getSubject().stringValue());
            if (this.useFragments)
                this.subject.setProperty(NTripleInputFormat.NAME, postProcess(s.getSubject()));
            this.subject.enablePath(this.enablePath);

            bb = ByteBuffer.wrap(md.digest(s.getObject().stringValue().getBytes()));
            long objectId = bb.getLong();
            this.object.reuse(objectId);
            this.object.setProperty(NTripleInputFormat.URI, s.getObject().stringValue());
            if (this.useFragments)
                this.object.setProperty(NTripleInputFormat.NAME, postProcess(s.getObject()));
            this.object.enablePath(this.enablePath);

            this.predicate.reuse(-1, subjectId, objectId, postProcess(s.getPredicate()));
            this.predicate.setProperty(NTripleInputFormat.URI, s.getPredicate().stringValue());
            if (null != s.getContext())
                this.predicate.setProperty(NTripleInputFormat.CONTEXT, s.getContext().stringValue());
            this.predicate.enablePath(this.enablePath);
        }
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
