package com.thinkaurelius.titan.core;

import com.tinkerpop.gremlin.process.computer.GraphComputer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanGraphComputer extends GraphComputer {

    public enum ResultMode { NONE, PERSIST, LOCALTX }

    public TitanGraphComputer setNumProcessingThreads(int threads);

    public TitanGraphComputer setResultMode(ResultMode mode);



}
