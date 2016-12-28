package org.janusgraph.diskstorage.util;

import java.util.concurrent.Callable;

import org.janusgraph.diskstorage.BackendException;

/**
 * Like {@link Callable}, except the exception type is narrowed from
 * {@link Exception} to {@link org.janusgraph.diskstorage.BackendException}.
 * 
 * @param <T>
 *            call return type
 */
public interface StorageCallable<T> extends Callable<T> {

    @Override
    public T call() throws BackendException;
}
