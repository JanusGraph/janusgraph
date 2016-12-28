package com.thinkaurelius.titan.olap;

import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;

import java.util.Optional;

public class ShortestDistanceMessageCombiner implements MessageCombiner<Long> {

    private static final Optional<ShortestDistanceMessageCombiner> INSTANCE = Optional.of(new
            ShortestDistanceMessageCombiner());

    private ShortestDistanceMessageCombiner() {

    }

    @Override
    public Long combine(final Long messageA, final Long messageB) {
        return Math.min(messageA, messageB);
    }

    public static Optional<ShortestDistanceMessageCombiner> instance() {
        return INSTANCE;
    }
}