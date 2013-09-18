package com.thinkaurelius.titan.graphdb

/**
 * This interface exists solely to avoid autoboxing primitive longs with Long.
 * 
 * A standard-library alternative to this interface is slated for JDK 8.
 * It currently exists in beta builds as java.util.stream.LongStream.LongIterator.
 */
interface LongIterator {
    long next()
    boolean hasNext()
}

class RandomLongIterator implements LongIterator {
    
    private long i = 0L
    private long offset
    private long count
    private long max
    private Random random
    
    RandomLongIterator(long count, long max, Random random) {
        this.count = count
        this.max = max
        this.offset = Math.abs(random.nextLong()) % max
    }
    
    long next() {
        (offset + i++) % max
    }
    
    boolean hasNext() {
        i < count
    }
}

class SequentialLongIterator implements LongIterator {
    
    private long i = 0L
    private long count
    
    SequentialLongIterator(long count) {
        this.count = count
    }
    
    long next() { i++ }
    boolean hasNext() { i < count }
}