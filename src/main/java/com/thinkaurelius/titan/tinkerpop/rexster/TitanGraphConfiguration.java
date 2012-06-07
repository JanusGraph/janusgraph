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

/**
 * Implements a Rexster GraphConfiguration for Titan
 *
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */

public class TitanGraphConfiguration implements GraphConfiguration {

    @Override
    public Graph configureGraphInstance(final Configuration properties) throws GraphConfigurationException {
        try {
            Configuration titanConfig = null;
            try {
                titanConfig = ((HierarchicalConfiguration) properties).configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
            } catch (IllegalArgumentException iae) {
                throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
            }
            Configuration rewriteConfig = new BaseConfiguration();
            if (properties.containsKey(Tokens.REXSTER_GRAPH_LOCATION)) {
                rewriteConfig.setProperty("storage.directory",properties.getString(Tokens.REXSTER_GRAPH_LOCATION));
            }
            if (properties.containsKey(Tokens.REXSTER_GRAPH_READ_ONLY)) {
                rewriteConfig.setProperty("storage.read-only",properties.getBoolean(Tokens.REXSTER_GRAPH_READ_ONLY));
            }
            return TitanFactory.open(new CompositeConfiguration(ImmutableList.of(rewriteConfig, titanConfig)));
        } catch (Exception e) {
            throw new GraphConfigurationException(e);
        }
    }
}
