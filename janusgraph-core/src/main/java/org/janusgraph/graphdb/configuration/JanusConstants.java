package org.janusgraph.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.janusgraph.core.JanusFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Collection of constants used throughput the Janus codebase.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusConstants {

    public static final String TITAN_PROPERTIES_FILE = "janus.internal.properties";

    /**
     * Runtime version of Janus, as read from a properties file inside the core jar
     */
    public static final String VERSION;

    /**
     * Past versions of Janus with which the runtime version shares a compatible storage format
     */
    public static final List<String> COMPATIBLE_VERSIONS;

    static {

        /*
         * Preempt some potential NPEs with Preconditions bearing messages. They
         * are unlikely to fail outside of some crazy test environment. Still,
         * if something goes horribly wrong, even a cryptic error message is
         * better than a message-less NPE.
         */
        Package p = JanusConstants.class.getPackage();
        Preconditions.checkNotNull(p, "Unable to load package containing class " + JanusConstants.class);
        String packageName = p.getName();
        Preconditions.checkNotNull(packageName, "Unable to get name of package containing " + JanusConstants.class);
        String resourceName = packageName.replace('.', '/') + "/" + TITAN_PROPERTIES_FILE;
        InputStream is = JanusFactory.class.getClassLoader().getResourceAsStream(resourceName);
        Preconditions.checkNotNull(is, "Unable to locate classpath resource " + resourceName + " containing Janus version");

        Properties props = new Properties();

        try {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + resourceName, e);
        }

        VERSION = props.getProperty("janus.version");
        ImmutableList.Builder<String> b = ImmutableList.builder();
        for (String v : props.getProperty("janus.compatible-versions", "").split(",")) {
            v = v.trim();
            if (!v.isEmpty()) b.add(v);
        }
        COMPATIBLE_VERSIONS = b.build();
    }

}
