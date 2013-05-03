package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Collection of constants used throughput the Titan codebase.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanConstants {
    
    public static final String TITAN_PROPERTIES_FILE = "titan.properties";

    /**
     * Version of this TitanGraph
     */
    public static final String VERSION;
    /**
     * Version numbers of compatible
     */
    public static final List<String> COMPATIBLE_VERSIONS;

    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream(TITAN_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        VERSION = props.getProperty("titan.version");
        ImmutableList.Builder<String> b = ImmutableList.builder();
        for (String v : props.getProperty("titan.compatible-versions", "").split(",")) {
            v = v.trim();
            if (!v.isEmpty()) b.add(v);
        }
        COMPATIBLE_VERSIONS = b.build();
    }

}
