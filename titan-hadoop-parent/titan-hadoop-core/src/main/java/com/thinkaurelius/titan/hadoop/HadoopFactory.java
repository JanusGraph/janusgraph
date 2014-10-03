package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.hadoop.config.HBaseAuthHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopFactory {

    public static HadoopGraph open(final String propertiesFile, final Configuration configuration) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return new HadoopGraph(configuration);
    }

    public static HadoopGraph open(final String propertiesFile) throws Exception {
        return HadoopFactory.open(propertiesFile, new Configuration());
    }
}
