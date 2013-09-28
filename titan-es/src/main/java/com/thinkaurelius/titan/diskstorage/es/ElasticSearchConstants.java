package com.thinkaurelius.titan.diskstorage.es;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanFactory;

public class ElasticSearchConstants {

    public static final String  ES_PROPERTIES_FILE = "titan-es.properties";
    public static final Version ES_VERSION_EXPECTED;
    
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConstants.class);
    
    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream(ES_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        
        String vs = props.getProperty("es.version");
        String versionConstantName = String.format("V_%s_%s_%s", (Object[])vs.split("\\."));
        ES_VERSION_EXPECTED = getExpectedVersionReflectively(versionConstantName);
    }
    
    private static Version getExpectedVersionReflectively(String fieldName) {
        final String msg = "Failed to load expected ES version";
        
        try {
            return (Version)Version.class.getField(fieldName).get(null);
        } catch (NoSuchFieldException e) {
            log.error(msg, e);
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            log.error(msg, e);
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            log.error(msg, e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            log.error(msg, e);
            throw new RuntimeException(e);
        }
    }
}
