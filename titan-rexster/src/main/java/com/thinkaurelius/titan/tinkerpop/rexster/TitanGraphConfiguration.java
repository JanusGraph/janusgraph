package com.thinkaurelius.titan.tinkerpop.rexster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a Rexster GraphConfiguration for Titan
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

public class TitanGraphConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(TitanGraphConfiguration.class);

//    @Override
//    public Graph configureGraphInstance(final GraphConfigurationContext context) throws GraphConfigurationException {
//        log.debug("Calling TitanFactory.open({})", context);
//        final Graph g = TitanFactory.open(convertConfiguration(context));
//        log.debug("Returning new graph {}", g);
//        return g;
//    }
//
//    public Configuration convertConfiguration(final GraphConfigurationContext context) throws GraphConfigurationException {
//        Configuration properties = context.getProperties();
//        try {
//            final Configuration titanConfig = new BaseConfiguration();
//            try {
//                final Configuration titanConfigProperties = ((HierarchicalConfiguration) properties).configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
//
//                final Iterator<String> titanConfigPropertiesKeys = titanConfigProperties.getKeys();
//                while (titanConfigPropertiesKeys.hasNext()) {
//                    String doubleDotKey = titanConfigPropertiesKeys.next();
//
//                    // replace the ".." put in play by apache commons configuration.  that's expected behavior
//                    // due to parsing key names to xml.
//                    String singleDotKey = doubleDotKey.replace("..", ".");
//
//                    // Combine multiple values into a comma-separated string
//                    String listVal = Joiner.on(AbstractConfiguration.getDefaultListDelimiter()).join(titanConfigProperties.getStringArray(doubleDotKey));
//
//                    titanConfig.setProperty(singleDotKey, listVal);
//                }
//            } catch (IllegalArgumentException iae) {
//                throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
//            }
//
//            final Configuration rewriteConfig = new BaseConfiguration();
//            final String location = properties.getString(Tokens.REXSTER_GRAPH_LOCATION, "");
//            if (titanConfig.getString("storage.backend").equals("berkeleyje") && location.trim().length() > 0) {
//                final File directory = new File(properties.getString(Tokens.REXSTER_GRAPH_LOCATION));
//                if (!directory.isDirectory()) {
//                    if (!directory.mkdirs()) {
//                        throw new IllegalArgumentException("Could not create directory: " + directory);
//                    }
//                }
//                rewriteConfig.setProperty("storage.directory", directory.toString());
//            }
//
//            if (properties.containsKey(Tokens.REXSTER_GRAPH_READ_ONLY)) {
//                rewriteConfig.setProperty("storage.read-only", properties.getBoolean(Tokens.REXSTER_GRAPH_READ_ONLY));
//            }
//
//            final CompositeConfiguration jointConfig = new CompositeConfiguration();
//            jointConfig.addConfiguration(rewriteConfig);
//            jointConfig.addConfiguration(titanConfig);
//            return jointConfig;
//        } catch (Exception e) {
//            //Unroll exceptions so that Rexster prints root cause
//            Throwable cause = e;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            throw new GraphConfigurationException(cause);
//        }
//    }
}
