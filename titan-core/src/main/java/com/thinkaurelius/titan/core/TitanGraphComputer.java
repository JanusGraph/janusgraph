package com.thinkaurelius.titan.core;

import com.tinkerpop.gremlin.process.computer.GraphComputer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphComputer extends GraphComputer {

    public enum ResultMode { NONE, PERSIST, LOCALTX }

    public TitanGraphComputer workers(int threads);

    public TitanGraphComputer resultMode(ResultMode mode);



}
