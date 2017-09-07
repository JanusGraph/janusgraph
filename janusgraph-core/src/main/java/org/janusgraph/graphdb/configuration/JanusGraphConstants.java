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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janusgraph.core.JanusGraphFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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

    /**
     * Past versions of Titan Graph with which the runtime version shares a compatible storage format
     */
    public static final List<String> TITAN_COMPATIBLE_VERSIONS;

    /**
     * Name of the ids.store-name used by JanusGraph which is configurable
     */
    public static final String JANUSGRAPH_ID_STORE_NAME = "janusgraph_ids";

    /**
     * Past name of the ids.store-name used by Titan Graph but which was not configurable
     */
    public static final String TITAN_ID_STORE_NAME = "titan_ids";

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

        Properties props = new Properties();

        try (InputStream is = JanusGraphFactory.class.getClassLoader().getResourceAsStream(resourceName)) {
            Preconditions.checkNotNull(is, "Unable to locate classpath resource " + resourceName + " containing JanusGraph version");
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + resourceName, e);
        }

        VERSION = props.getProperty("janusgraph.version");
        COMPATIBLE_VERSIONS = getCompatibleVersions(props, "janusgraph.compatible-versions");
        TITAN_COMPATIBLE_VERSIONS = getCompatibleVersions(props, "titan.compatible-versions");
    }

    static List<String> getCompatibleVersions(Properties props, String key) {
        ImmutableList.Builder<String> b = ImmutableList.builder();
        b.addAll(Stream.of(props.getProperty(key, "").split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
        return b.build();
    }
}
