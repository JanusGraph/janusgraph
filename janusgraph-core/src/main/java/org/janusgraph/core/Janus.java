package org.janusgraph.core;

import org.janusgraph.graphdb.configuration.JanusConstants;

import org.apache.tinkerpop.gremlin.util.Gremlin;


/**
 * Contains constants for this Janus Graph Database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Janus {

    /**
     * The version of this Janus graph database
     *
     * @return
     */
    public static String version() {
        return JanusConstants.VERSION;
    }

    public static void main(String[] args) {
        System.out.println("Janus " + Janus.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
