// Copyright 2023 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util;

import org.janusgraph.core.JanusGraphException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class CompletableFutureUtil {

    private CompletableFutureUtil(){}

    /**
     * If possible, returns the first root cause exception which is not `ExecutionException` and not `CompletionException`.
     * Notice, that during the traversal process any suppressed parent's exceptions are added to the child exception.
     * Thus, this function not only searches for the root cause exception but also modifies it's suppressed exceptions list
     * to include all suppressed parent's exceptions.
     */
    public static Throwable unwrapExecutionException(Throwable e){
        // Unwrap any ExecutionExceptions to get to the real cause:
        Throwable rootException = e;
        while (rootException.getCause() != null && (rootException instanceof ExecutionException || rootException instanceof CompletionException)){
            Throwable cause = rootException.getCause();
            for(Throwable suppressedException : rootException.getSuppressed()){
                cause.addSuppressed(suppressedException);
            }
            rootException = cause;
        }
        return rootException;
    }

    public static <T> T get(CompletableFuture<T> future){
        try {
            return future.get();
        } catch (Throwable e) {
            Throwable rootException = unwrapExecutionException(e);
            if(rootException instanceof InterruptedException){
                Thread.currentThread().interrupt();
            } else if(rootException.getCause() instanceof InterruptedException){
                rootException = rootException.getCause();
            }
            throw new JanusGraphException(rootException);
        }
    }

    public static <K,V> Map<K,V> unwrap(Map<K,CompletableFuture<V>> futureMap) throws Throwable{
        Map<K, V> resultMap = new HashMap<>(futureMap.size());
        Throwable firstException = null;
        for(Map.Entry<K, CompletableFuture<V>> entry : futureMap.entrySet()){
            try{
                resultMap.put(entry.getKey(), entry.getValue().get());
            } catch (Throwable throwable){
                if(firstException == null){
                    firstException = throwable;
                } else {
                    firstException.addSuppressed(throwable);
                }
            }
        }

        if(firstException != null){
            throw firstException;
        }
        return resultMap;
    }

    public static <V> void awaitAll(Collection<CompletableFuture<V>> futureCollection) throws Throwable{
        Throwable firstException = null;
        for(CompletableFuture<V> future : futureCollection){
            try{
                future.get();
            } catch (Throwable throwable){
                if(firstException == null){
                    firstException = throwable;
                } else {
                    firstException.addSuppressed(throwable);
                }
            }
        }

        if(firstException != null){
            throw firstException;
        }
    }

}
