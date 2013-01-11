package com.thinkaurelius.faunus.formats.edgelist.rdf;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.tinkerpop.blueprints.impls.sail.SailTokens;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFBlueprintsHandler implements RDFHandler, Iterator<FaunusElement> {

    private final MessageDigest md;
    private final boolean enablePath;
    private final boolean useFragments;
    private final Set<String> asProperties = new HashSet<String>();
    private final boolean literalAsProperty;
    private static final String BASE_URI = "http://thinkaurelius.com#";

    private RDFParser parser;
    private final Queue<FaunusElement> queue = new LinkedList<FaunusElement>();
    public static final Map<String, RDFFormat> formats = new HashMap<String, RDFFormat>();

    private static Map<String, String> dataTypeToClass = new HashMap<String, String>();

    static {
        dataTypeToClass.put(SailTokens.XSD_NS + "string", "java.lang.String");
        dataTypeToClass.put(SailTokens.XSD_NS + "int", "java.lang.Integer");
        dataTypeToClass.put(SailTokens.XSD_NS + "integer", "java.lang.Integer");
        dataTypeToClass.put(SailTokens.XSD_NS + "float", "java.lang.Float");
        dataTypeToClass.put(SailTokens.XSD_NS + "double", "java.lang.Double");
    }

    static {
        formats.put("rdf-xml", RDFFormat.RDFXML);
        formats.put("n-triples", RDFFormat.NTRIPLES);
        formats.put("turtle", RDFFormat.TURTLE);
        formats.put("n3", RDFFormat.N3);
        formats.put("trix", RDFFormat.TRIX);
        formats.put("trig", RDFFormat.TRIG);
        //formats.put("n-quads", NQuadsFormat.NQUADS);
    }

    public RDFBlueprintsHandler(final Configuration configuration) throws IOException {
        this.enablePath = configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false);
        this.useFragments = configuration.getBoolean(RDFInputFormat.USE_LOCALNAME, false);
        this.literalAsProperty = configuration.getBoolean(RDFInputFormat.LITERAL_AS_PROPERTY, false);
        for (final String property : configuration.getStringCollection(RDFInputFormat.AS_PROPERTIES)) {
            this.asProperties.add(property);
        }
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        }

        this.parser = Rio.createParser(formats.get(configuration.get(RDFInputFormat.RDF_FORMAT)));
        this.parser.setRDFHandler(this);
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

    private static Object castLiteral(final Literal literal) {
        if (null != literal.getDatatype()) {
            String className = dataTypeToClass.get(literal.getDatatype().stringValue());
            if (null == className)
                return literal.getLabel();
            else {
                try {
                    Class c = Class.forName(className);
                    if (c == String.class) {
                        return literal.getLabel();
                    } else if (c == Float.class) {
                        return Float.valueOf(literal.getLabel());
                    } else if (c == Integer.class) {
                        return Integer.valueOf(literal.getLabel());
                    } else if (c == Double.class) {
                        return Double.valueOf(literal.getLabel());
                    } else {
                        return literal.getLabel();
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return literal.getLabel();
                }
            }
        } else {
            return literal.getLabel();
        }
    }

    public void handleStatement(final Statement s) throws RDFHandlerException {
        if (this.asProperties.contains(s.getPredicate().toString())) {
            final ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
            final FaunusVertex subject = new FaunusVertex(bb.getLong());
            subject.setProperty(postProcess(s.getPredicate()), postProcess(s.getObject()));
            subject.setProperty(RDFInputFormat.URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(RDFInputFormat.NAME, postProcess(s.getSubject()));
            subject.enablePath(this.enablePath);
            this.queue.add(subject);
        } else if (this.literalAsProperty && (s.getObject() instanceof Literal)) {
            final ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
            final FaunusVertex subject = new FaunusVertex(bb.getLong());
            subject.setProperty(postProcess(s.getPredicate()), castLiteral((Literal) s.getObject()));
            subject.setProperty(RDFInputFormat.URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(RDFInputFormat.NAME, postProcess(s.getSubject()));
            subject.enablePath(this.enablePath);
            this.queue.add(subject);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(md.digest(s.getSubject().stringValue().getBytes()));
            long subjectId = bb.getLong();
            final FaunusVertex subject = new FaunusVertex(subjectId);
            subject.reuse(subjectId);
            subject.setProperty(RDFInputFormat.URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(RDFInputFormat.NAME, postProcess(s.getSubject()));
            subject.enablePath(this.enablePath);
            this.queue.add(subject);

            bb = ByteBuffer.wrap(md.digest(s.getObject().stringValue().getBytes()));
            long objectId = bb.getLong();
            final FaunusVertex object = new FaunusVertex(objectId);
            object.reuse(objectId);
            object.setProperty(RDFInputFormat.URI, s.getObject().stringValue());
            if (this.useFragments)
                object.setProperty(RDFInputFormat.NAME, postProcess(s.getObject()));
            object.enablePath(this.enablePath);
            this.queue.add(object);

            final FaunusEdge predicate = new FaunusEdge(-1, subjectId, objectId, postProcess(s.getPredicate()));
            predicate.setProperty(RDFInputFormat.URI, s.getPredicate().stringValue());
            if (null != s.getContext())
                predicate.setProperty(RDFInputFormat.CONTEXT, s.getContext().stringValue());
            predicate.enablePath(this.enablePath);
            this.queue.add(predicate);
        }
    }

    public void handleComment(String s) throws RDFHandlerException {
        // Do nothing
    }

    public boolean parse(final String string) throws IOException {
        if (null == string)
            return false;
        try {
            this.parser.parse(new StringReader(string), BASE_URI);
            return true;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public FaunusElement next() {
        if (this.queue.isEmpty())
            return null;
        else
            return this.queue.remove();
    }

    public boolean hasNext() {
        return !this.queue.isEmpty();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
