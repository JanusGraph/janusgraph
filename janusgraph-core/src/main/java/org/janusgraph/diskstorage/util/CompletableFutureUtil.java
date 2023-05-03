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

public class CompletableFutureUtil {

    public static <T> T get(CompletableFuture<T> future){
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JanusGraphException(e);
        } catch (Throwable e) {
            if(e.getCause() instanceof InterruptedException){
                throw new JanusGraphException(e.getCause());
            }
            throw new JanusGraphException(e);
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
