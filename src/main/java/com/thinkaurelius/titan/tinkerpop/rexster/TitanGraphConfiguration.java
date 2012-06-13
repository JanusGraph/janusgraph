package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.util.Iterator;

/**
 * Implements a Rexster GraphConfiguration for Titan
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

public class TitanGraphConfiguration implements GraphConfiguration {

    @Override
    public Graph configureGraphInstance(final Configuration properties) throws GraphConfigurationException {
        return TitanFactory.open(convertConfiguration(properties));
    }

    public Configuration convertConfiguration(final Configuration properties) throws GraphConfigurationException {
        try {
            final Configuration titanConfig = new BaseConfiguration();
            try {
                final Configuration titanConfigProperties = ((HierarchicalConfiguration) properties).configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);

                final Iterator<String> titanConfigPropertiesKeys = titanConfigProperties.getKeys();
                while (titanConfigPropertiesKeys.hasNext()) {
                    String key = titanConfigPropertiesKeys.next();

                    // replace the ".." put in play by apache commons configuration.  that's expected behavior
                    // due to parsing key names to xml.
                    titanConfig.setProperty(key.replace("..", "."), titanConfigProperties.getString(key));
                }
            } catch (IllegalArgumentException iae) {
                throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
            }

            final Configuration rewriteConfig = new BaseConfiguration();
            if (properties.containsKey(Tokens.REXSTER_GRAPH_LOCATION)) {
                rewriteConfig.setProperty("storage.directory",properties.getString(Tokens.REXSTER_GRAPH_LOCATION));
            }
            if (properties.containsKey(Tokens.REXSTER_GRAPH_READ_ONLY)) {
                rewriteConfig.setProperty("storage.read-only",properties.getBoolean(Tokens.REXSTER_GRAPH_READ_ONLY));
            }

            final CompositeConfiguration jointConfig = new CompositeConfiguration();
            jointConfig.addConfiguration(rewriteConfig);
            jointConfig.addConfiguration(titanConfig);
            return jointConfig;
        } catch (Exception e) {
            throw new GraphConfigurationException(e);
        }
    }
}
