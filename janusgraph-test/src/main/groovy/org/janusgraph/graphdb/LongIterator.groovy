package org.janusgraph.graphdb

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

/**
 * Returns a sequence of longs modulo some fixed limit and starting at some fixed value.
 */
class SequentialLongIterator implements LongIterator {
    
    private long i = 0L
    private long offset
    private long count
    private long max
    
    SequentialLongIterator(long count, long max, long offset) {
        this.count = count
        this.max = max
        this.offset = offset
    }
    
    long next() {
        (offset + i++) % max
    }
    
    boolean hasNext() {
        i < count
    }
}
