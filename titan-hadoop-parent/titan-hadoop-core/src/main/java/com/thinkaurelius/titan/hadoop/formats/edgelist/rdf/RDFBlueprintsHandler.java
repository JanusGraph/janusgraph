package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.FaunusElement;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.tinkerpop.blueprints.impls.sail.SailTokens;

import org.apache.log4j.Logger;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static com.thinkaurelius.titan.hadoop.formats.edgelist.rdf.RDFConfig.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFBlueprintsHandler implements RDFHandler, Iterator<FaunusElement> {

    private static final Logger logger = Logger.getLogger(RDFBlueprintsHandler.class);

    private final boolean useFragments;
    private final ModifiableHadoopConfiguration faunusConf;
    private final Configuration rdfConf;
    private final Set<String> asProperties = new HashSet<String>();
    private final boolean literalAsProperty;
    private final RDFParser parser;
    private final Queue<FaunusElement> queue = new LinkedList<FaunusElement>();
    private final String baseURI;
    private final Set<String> reservedFragments;

    // Immutable/constant data
    private static ImmutableMap<String, Character> dataTypeToClass;
    private static final char STRING = 's';
    private static final char INTEGER = 'i';
    private static final char FLOAT = 'f';
    private static final char DOUBLE = 'd';
    private static final char LONG = 'l';
    private static final char BOOLEAN = 'b';

    static {
        ImmutableMap.Builder<String, Character> b = ImmutableMap.builder();
        b.put(SailTokens.XSD_NS + "string", STRING);
        b.put(SailTokens.XSD_NS + "int", INTEGER);
        b.put(SailTokens.XSD_NS + "integer", INTEGER);
        b.put(SailTokens.XSD_NS + "float", FLOAT);
        b.put(SailTokens.XSD_NS + "double", DOUBLE);
        b.put(SailTokens.XSD_NS + "long", LONG);
        b.put(SailTokens.XSD_NS + "boolean", BOOLEAN);
        dataTypeToClass = b.build();
    }

    public RDFBlueprintsHandler(final ModifiableHadoopConfiguration configuration) throws IOException {


        // exclude fragments which are most likely to interfere in a Titan/Faunus pipeline
        reservedFragments = new HashSet<String>();
        reservedFragments.add("label");
        //reservedFragments.add("type");
        reservedFragments.add("id");

        faunusConf = configuration;
        rdfConf = faunusConf.getInputConf(ROOT_NS);

        this.baseURI = rdfConf.get(RDF_BASE_URI);
        this.useFragments = rdfConf.get(RDF_USE_LOCALNAME);
        this.literalAsProperty = rdfConf.get(RDF_LITERAL_AS_PROPERTY);
        for (final String property : rdfConf.get(RDF_AS_PROPERTIES)) {
            this.asProperties.add(property.trim());
        }

        if (!rdfConf.has(RDF_FORMAT)) {
            throw new RuntimeException("RDF format is required.  Set " + ConfigElement.getPath(TitanHadoopConfiguration.INPUT_CONF_NS) + "." + RDF_FORMAT.getName());
        }
        Syntax syntax = rdfConf.get(RDF_FORMAT);
        RDFFormat format = syntax.getRDFFormat();
        Preconditions.checkNotNull(format);

        this.parser = Rio.createParser(format);

        this.parser.setRDFHandler(this);
        this.parser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
    }

    public void startRDF() throws RDFHandlerException {
        // Do nothing
    }

    public void endRDF() throws RDFHandlerException {
    }

    public void handleNamespace(String s, String s1) throws RDFHandlerException {
        // Do nothing
    }

    public String postProcess(final Value resource) {
        if (resource instanceof URI) {
            if (this.useFragments) {
                return createFragment(resource);
            } else {
                return resource.stringValue();
            }
        } else {
            return resource.stringValue();
        }
    }

    /**
     * Simplifies the lexical representation of a value, in particular by taking the fragment identifier of URI
     * This is a lossy operation; many distinct URIs may map to the same fragment.
     * Conflicts with reserved tokens are avoided.
     *
     * @param resource the Value to map
     * @return the simplified fragment
     */
    private String createFragment(final Value resource) {
        if (resource instanceof URI) {
            String frag = ((URI) resource).getLocalName();
            return reservedFragments.contains(frag) ? frag + "_" : frag;
        } else {
            return resource.stringValue();
        }
    }

    private static Object castLiteral(final Literal literal) {
        if (null != literal.getDatatype()) {
            final Character type = dataTypeToClass.get(literal.getDatatype().stringValue());
            if (null == type)
                return literal.getLabel();
            else {
                if (STRING == type) {
                    return literal.getLabel();
                } else if (FLOAT == type) {
                    return Float.valueOf(literal.getLabel());
                } else if (INTEGER == type) {
                    return Integer.valueOf(literal.getLabel());
                } else if (DOUBLE == type) {
                    return Double.valueOf(literal.getLabel());
                } else if (LONG == type) {
                    return Long.valueOf(literal.getLabel());
                } else if (BOOLEAN == type) {
                    return Boolean.valueOf(literal.getLabel());
                } else {
                    return literal.getLabel();
                }
            }
        } else {
            return literal.getLabel();
        }
    }

    public void handleStatement(final Statement s) throws RDFHandlerException {
        if (this.asProperties.contains(s.getPredicate().toString())) {
            final FaunusVertex subject = new FaunusVertex(faunusConf, Crc64.digest(s.getSubject().stringValue().getBytes()));
            subject.setProperty(postProcess(s.getPredicate()), postProcess(s.getObject()));
            subject.setProperty(URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(NAME, createFragment(s.getSubject()));
            this.queue.add(subject);
        } else if (this.literalAsProperty && (s.getObject() instanceof Literal)) {
            final FaunusVertex subject = new FaunusVertex(faunusConf, Crc64.digest(s.getSubject().stringValue().getBytes()));
            subject.setProperty(postProcess(s.getPredicate()), castLiteral((Literal) s.getObject()));
            subject.setProperty(URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(NAME, createFragment(s.getSubject()));
            this.queue.add(subject);
        } else {
            long subjectId = Crc64.digest(s.getSubject().stringValue().getBytes());
            final FaunusVertex subject = new FaunusVertex(faunusConf, subjectId);
            subject.setProperty(URI, s.getSubject().stringValue());
            if (this.useFragments)
                subject.setProperty(NAME, createFragment(s.getSubject()));
            this.queue.add(subject);

            long objectId = Crc64.digest(s.getObject().stringValue().getBytes());
            final FaunusVertex object = new FaunusVertex(faunusConf, objectId);
            object.setProperty(URI, s.getObject().stringValue());
            if (this.useFragments)
                object.setProperty(NAME, createFragment(s.getObject()));
            this.queue.add(object);

            final StandardFaunusEdge predicate = new StandardFaunusEdge(faunusConf, -1, subjectId, objectId, postProcess(s.getPredicate()));
            predicate.setProperty(URI, s.getPredicate().stringValue());
            if (null != s.getContext())
                predicate.setProperty(CONTEXT, s.getContext().stringValue());
            // TODO predicate.enablePath(this.enablePath);
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
            this.parser.parse(new StringReader(string), baseURI);
            return true;
        } catch (Exception e) {
            this.logger.error(e.getMessage());
            e.printStackTrace();
            return false;
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
