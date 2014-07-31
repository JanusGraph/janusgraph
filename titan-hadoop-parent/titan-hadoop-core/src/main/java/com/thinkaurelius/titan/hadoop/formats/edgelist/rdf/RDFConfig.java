package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import org.openrdf.rio.RDFFormat;

import javax.annotation.Nullable;

public class RDFConfig {

    public static final String URI = "uri";
    public static final String CONTEXT = "context";
    public static final String NAME = "name";

    public static enum Syntax {
        RDF_XML(RDFFormat.RDFXML),
        N_TRIPLES(RDFFormat.NTRIPLES),
        TURTLE(RDFFormat.TURTLE),
        N3(RDFFormat.N3),
        TRIX(RDFFormat.TRIX),
        TRIG(RDFFormat.TRIG);

        private final RDFFormat fmt;

        private Syntax(RDFFormat fmt) {
            this.fmt = fmt;
        }

        public RDFFormat getRDFFormat() {
            return fmt;
        }
    }

    public static final ConfigNamespace RDF_ROOT = new ConfigNamespace(null, "rdf", "RDF MapReduce format options");

    public static final ConfigOption<Syntax> RDF_FORMAT =
            new ConfigOption<Syntax>(RDF_ROOT, "format",
            "The format/syntax/dialect of the RDF input file(s)",
            ConfigOption.Type.LOCAL, Syntax.class);

    public static final ConfigOption<Boolean> RDF_USE_LOCALNAME =
            new ConfigOption<Boolean>(RDF_ROOT, "use-localname",
            "Whether to tolerate fragments when parsing RDF input",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> RDF_LITERAL_AS_PROPERTY =
            new ConfigOption<Boolean>(RDF_ROOT, "literal-as-property",
            "Whether to turn RDF triples where the object is a literal value into " +
            "properties on the subject, where the property name is the predicate and " +
            "the property value is the literal object.",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String[]> RDF_AS_PROPERTIES =
            new ConfigOption<String[]>(RDF_ROOT, "as-properties",
            "A comma-separated list of predicate URIs for which matching triples " +
            "will be converted into properties instead of edges",
            ConfigOption.Type.LOCAL, String[].class, new String[]{},
            new Predicate<String[]>() {
                @Override
                public boolean apply(@Nullable String[] input) {
                    // Zero-length array is allowed (and the default), but null disallowed
                    return null != input;
                }
            });

    public static final ConfigOption<String> RDF_BASE_URI =
            new ConfigOption<String>(RDF_ROOT, "base-uri",
            "The URI used to resolve any relative URI references encountered in the input",
            ConfigOption.Type.LOCAL, "http://thinkaurelius.com#");
}
