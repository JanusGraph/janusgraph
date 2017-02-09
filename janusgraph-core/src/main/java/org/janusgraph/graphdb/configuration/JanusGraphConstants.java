// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.janusgraph.core.JanusGraphFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Collection of constants used throughput the JanusGraph codebase.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphConstants {

    public static final String JANUSGRAPH_PROPERTIES_FILE = "janusgraph.internal.properties";

    /**
     * Runtime version of JanusGraph, as read from a properties file inside the core jar
     */
    public static final String VERSION;

    /**
     * Past versions of JanusGraph with which the runtime version shares a compatible storage format
     */
    public static final List<String> COMPATIBLE_VERSIONS;

    static {

        /*
         * Preempt some potential NPEs with Preconditions bearing messages. They
         * are unlikely to fail outside of some crazy test environment. Still,
         * if something goes horribly wrong, even a cryptic error message is
         * better than a message-less NPE.
         */
        Package p = JanusGraphConstants.class.getPackage();
        Preconditions.checkNotNull(p, "Unable to load package containing class " + JanusGraphConstants.class);
        String packageName = p.getName();
        Preconditions.checkNotNull(packageName, "Unable to get name of package containing " + JanusGraphConstants.class);
        String resourceName = packageName.replace('.', '/') + "/" + JANUSGRAPH_PROPERTIES_FILE;
        InputStream is = JanusGraphFactory.class.getClassLoader().getResourceAsStream(resourceName);
        Preconditions.checkNotNull(is, "Unable to locate classpath resource " + resourceName + " containing JanusGraph version");

        Properties props = new Properties();

        try {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + resourceName, e);
        }

        VERSION = props.getProperty("janusgraph.version");
        ImmutableList.Builder<String> b = ImmutableList.builder();
        for (String v : props.getProperty("janusgraph.compatible-versions", "").split(",")) {
            v = v.trim();
            if (!v.isEmpty()) b.add(v);
        }
        COMPATIBLE_VERSIONS = b.build();
    }

}
