package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;


/**
 * Contains constants for this Titan Graph Database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Titan {

    public static final String TTL = "_ttl";

    /**
     * The version of this Titan graph database
     *
     * @return
     */
    public static String version() {
        return TitanConstants.VERSION;
    }
}
