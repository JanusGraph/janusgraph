package com.thinkaurelius.titan.diskstorage.es;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanFactory;

public class ElasticSearchConstants {

    public static final String ES_PROPERTIES_FILE = "titan-es.properties";
    public static final String ES_VERSION_EXPECTED;
    
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConstants.class);
    
    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream(ES_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        ES_VERSION_EXPECTED = props.getProperty("es.version");
    }
}
