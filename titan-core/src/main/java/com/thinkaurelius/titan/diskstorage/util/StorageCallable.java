package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.Callable;

import com.thinkaurelius.titan.diskstorage.BackendException;

/**
 * Like {@link Callable}, except the exception type is narrowed from
 * {@link Exception} to {@link com.thinkaurelius.titan.diskstorage.BackendException}.
 * 
 * @param <T>
 *            call return type
 */
public interface StorageCallable<T> extends Callable<T> {

    @Override
    public T call() throws BackendException;
}
