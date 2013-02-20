package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;


/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */
public class Titan {


    public static String version() {
        return TitanConstants.VERSION;
    }

    public static final class Token {

        public static final String STANDARD_INDEX = "standard";

    }
}
