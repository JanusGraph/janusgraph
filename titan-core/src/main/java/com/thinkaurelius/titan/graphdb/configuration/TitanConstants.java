package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanFactory;

import java.io.IOException;
import java.io.InputStream;
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

        /*
         * Preempt some potential NPEs with Preconditions bearing messages. They
         * are unlikely to fail outside of some crazy test environment. Still,
         * if something goes horribly wrong, even a cryptic error message is
         * better than a message-less NPE.
         */
        Package p = TitanConstants.class.getPackage();
        Preconditions.checkNotNull(p, "Unable to load package containing class " + TitanConstants.class);
        String packageName = p.getName();
        Preconditions.checkNotNull(packageName, "Unable to get name of package containing " + TitanConstants.class);
        String resourceName = packageName.replace('.', '/') + "/" + TITAN_PROPERTIES_FILE;
        InputStream is = TitanFactory.class.getClassLoader().getResourceAsStream(resourceName);
        Preconditions.checkNotNull(is, "Unable to locate classpath resource " + resourceName + " containing Titan version");

        Properties props = new Properties();

        try {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + resourceName, e);
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
