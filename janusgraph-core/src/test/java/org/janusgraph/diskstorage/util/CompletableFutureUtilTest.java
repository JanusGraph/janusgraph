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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.util.CompletableFutureUtil.awaitAll;
import static org.janusgraph.diskstorage.util.CompletableFutureUtil.get;
import static org.janusgraph.diskstorage.util.CompletableFutureUtil.unwrap;
import static org.janusgraph.diskstorage.util.CompletableFutureUtil.unwrapExecutionException;

public class CompletableFutureUtilTest {

    @Test
    public void shouldUnwrapExecutionException(){
        IllegalStateException originalException = new IllegalStateException();
        Assertions.assertEquals(originalException, unwrapExecutionException(new ExecutionException(originalException)));
        Assertions.assertEquals(originalException, unwrapExecutionException(new CompletionException(originalException)));
        Assertions.assertEquals(originalException, unwrapExecutionException(originalException));

        Throwable suppressedException1 = new ArrayIndexOutOfBoundsException();
        Throwable suppressedException2 = new IllegalArgumentException();
        Throwable suppressedException3 = new IllegalStateException();
        Throwable suppressedException4 = new NullPointerException();
        Throwable suppressedException5 = new ClassCastException();
        Set<Throwable> allSuppressedExceptions = new HashSet<>();
        allSuppressedExceptions.add(suppressedException1);
        allSuppressedExceptions.add(suppressedException2);
        allSuppressedExceptions.add(suppressedException3);
        allSuppressedExceptions.add(suppressedException4);
        allSuppressedExceptions.add(suppressedException5);

        originalException = new IllegalStateException();
        originalException.addSuppressed(suppressedException1);
        ExecutionException executionException1 = new ExecutionException(originalException);
        executionException1.addSuppressed(suppressedException2);
        executionException1.addSuppressed(suppressedException3);
        ExecutionException executionException2 = new ExecutionException(executionException1);
        executionException2.addSuppressed(suppressedException4);
        executionException2.addSuppressed(suppressedException5);

        Assertions.assertEquals(1, originalException.getSuppressed().length);
        Assertions.assertEquals(originalException, unwrapExecutionException(executionException2));
        Assertions.assertEquals(5, originalException.getSuppressed().length);
        for(Throwable suppressedException : originalException.getSuppressed()){
            allSuppressedExceptions.remove(suppressedException);
        }
        Assertions.assertTrue(allSuppressedExceptions.isEmpty());
    }

    @Test
    public void shouldGetCompletedFuture(){
        Object expected = new Object();
        Assertions.assertEquals(expected, get(CompletableFuture.completedFuture(expected)));
    }

    @Test
    public void shouldWrapInterruptedExceptionIntoJanusGraphException(){
        CompletableFuture<Object> directInterruptedExceptionFuture = new CompletableFuture<>();
        directInterruptedExceptionFuture.completeExceptionally(new InterruptedException());
        JanusGraphException exception = Assertions.assertThrows(JanusGraphException.class, () -> get(directInterruptedExceptionFuture));
        Assertions.assertTrue(exception.getCause() instanceof InterruptedException);

        CompletableFuture<Object> nestedInterruptedExceptionFuture = new CompletableFuture<>();
        nestedInterruptedExceptionFuture.completeExceptionally(new Exception(new InterruptedException()));
        exception = Assertions.assertThrows(JanusGraphException.class, () -> get(nestedInterruptedExceptionFuture));
        Assertions.assertTrue(exception.getCause() instanceof InterruptedException);
    }

    @Test
    public void shouldWrapOriginalExceptionIntoJanusGraphException(){
        CompletableFuture<Object> directInterruptedExceptionFuture = new CompletableFuture<>();
        directInterruptedExceptionFuture.completeExceptionally(new IllegalStateException());
        JanusGraphException exception = Assertions.assertThrows(JanusGraphException.class, () -> get(directInterruptedExceptionFuture));
        Assertions.assertTrue(exception.getCause() instanceof IllegalStateException);
    }

    @Test
    public void shouldUnwrapMap() throws Throwable {
        Map<Object,CompletableFuture<Object>> futureMap = new HashMap<>();
        for(int i=0; i<10; i++){
            futureMap.put(new Object(), CompletableFuture.completedFuture(new Object()));
        }
        Map<Object,Object> resultMap = unwrap(futureMap);
        Assertions.assertEquals(futureMap.size(), resultMap.size());
        for(Map.Entry<Object, Object> resultEntry : resultMap.entrySet()){
            Assertions.assertEquals(resultEntry.getValue(), futureMap.get(resultEntry.getKey()).get());
        }
    }

    @Test
    public void shouldFailMapUnwrapWithSuppressedExceptions() {
        Map<Object,CompletableFuture<Object>> futureMap = new LinkedHashMap<>();
        CompletableFuture firstFuture = new CompletableFuture();
        firstFuture.completeExceptionally(new IllegalStateException());
        futureMap.put(new Object(), firstFuture);
        for(int i=0; i<10; i++){
            CompletableFuture otherFuture = new CompletableFuture();
            otherFuture.completeExceptionally(new IllegalArgumentException());
            futureMap.put(new Object(), otherFuture);
        }
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> unwrap(futureMap));
        Throwable[] suppressedExceptions = exception.getSuppressed();
        Assertions.assertEquals(10, suppressedExceptions.length);
        for(int i=0; i<10; i++){
            Assertions.assertTrue(suppressedExceptions[i] instanceof IllegalArgumentException);
        }
    }

    @Test
    public void shouldAwaitListOfFutures() {
        List<CompletableFuture<Object>> futureList = new ArrayList<>(10);
        for(int i=0; i<10; i++){
            futureList.add(CompletableFuture.completedFuture(new Object()));
        }
        Assertions.assertDoesNotThrow(() -> awaitAll(futureList));
    }

    @Test
    public void shouldFailCollectionAwaitWithSuppressedExceptions() {
        List<CompletableFuture<Object>> futureList = new ArrayList<>(10);
        CompletableFuture firstFuture = new CompletableFuture();
        firstFuture.completeExceptionally(new IllegalStateException());
        futureList.add(firstFuture);
        for(int i=0; i<10; i++){
            CompletableFuture otherFuture = new CompletableFuture();
            otherFuture.completeExceptionally(new IllegalArgumentException());
            futureList.add(otherFuture);
        }
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> awaitAll(futureList));
        Throwable[] suppressedExceptions = exception.getSuppressed();
        Assertions.assertEquals(10, suppressedExceptions.length);
        for(int i=0; i<10; i++){
            Assertions.assertTrue(suppressedExceptions[i] instanceof IllegalArgumentException);
        }
    }

}
