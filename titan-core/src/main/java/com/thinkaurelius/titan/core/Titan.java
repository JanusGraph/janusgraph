package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;

import org.apache.tinkerpop.gremlin.util.Gremlin;


/**
 * Contains constants for this Titan Graph Database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Titan {

    /**
     * The version of this Titan graph database
     *
     * @return
     */
    public static String version() {
        return TitanConstants.VERSION;
    }

    public static void main(String[] args) {
        System.out.println("Titan " + Titan.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
