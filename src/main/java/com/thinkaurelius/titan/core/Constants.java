package com.thinkaurelius.titan.core;

import java.io.IOException;
import java.util.Properties;

public class Constants {
    public static final String VERSION;

    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream("titan.properties"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        VERSION = props.getProperty("titan.version");
    }
}
