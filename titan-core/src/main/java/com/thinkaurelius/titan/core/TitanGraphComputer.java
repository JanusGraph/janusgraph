package com.thinkaurelius.titan.core;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphComputer extends GraphComputer {

    public enum ResultMode {
        NONE, PERSIST, LOCALTX;

        public ResultGraph toResultGraph() {
            switch(this) {
                case NONE: return ResultGraph.ORIGINAL;
                case PERSIST: return ResultGraph.ORIGINAL;
                case LOCALTX: return ResultGraph.NEW;
                default: throw new AssertionError("Unrecognized option: " + this);
            }
        }

        public Persist toPersist() {
            switch(this) {
                case NONE: return Persist.NOTHING;
                case PERSIST: return Persist.VERTEX_PROPERTIES;
                case LOCALTX: return Persist.VERTEX_PROPERTIES;
                default: throw new AssertionError("Unrecognized option: " + this);
            }
        }

    }

    public TitanGraphComputer workers(int threads);

    public default TitanGraphComputer resultMode(ResultMode mode) {
        result(mode.toResultGraph());
        persist(mode.toPersist());
        return this;
    }



}
