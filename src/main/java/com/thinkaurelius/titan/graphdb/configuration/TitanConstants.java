package com.thinkaurelius.titan.graphdb.configuration;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanFactory;

import java.io.IOException;
import java.util.*;

public class TitanConstants {
    public static final String VERSION;
    public static final List<String> COMPATIBLE_VERSIONS;

    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream("titan.properties"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        VERSION = props.getProperty("titan.version");
        ImmutableList.Builder<String> b = ImmutableList.builder();
        for (String v : props.getProperty("titan.compatible-versions","").split(",")) {
            v = v.trim();
            if (!v.isEmpty()) b.add(v);
        }
        COMPATIBLE_VERSIONS = b.build();
    }

}
