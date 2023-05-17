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

package org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchProcessingQueueTest {

    @Test
    void addToBatchToEnd() {
        BatchProcessingQueue<Integer> queue = new BatchProcessingQueue<>(3);
        queue.addToBatchToEnd(1);
        queue.addToBatchToEnd(2);
        queue.addToBatchToEnd(3);
        assertEquals(3, queue.removeFirst().size());
        assertEquals(0, queue.removeFirst().size());
    }

    @Test
    void removeFirst() {
        BatchProcessingQueue<Integer> queue = new BatchProcessingQueue<>(3);
        queue.addToBatchToEnd(1);
        queue.addToBatchToEnd(2);
        queue.addToBatchToEnd(3);
        Collection<Integer> batch = queue.removeFirst();
        assertEquals(Arrays.asList(1, 2, 3), batch);
        assertEquals(0, queue.removeFirst().size());
    }

    @Test
    void removeLast() {
        BatchProcessingQueue<Integer> queue = new BatchProcessingQueue<>(3);
        queue.addToBatchToEnd(1);
        queue.addToBatchToEnd(2);
        queue.addToBatchToEnd(3);
        Collection<Integer> batch = queue.removeLast();
        assertEquals(Arrays.asList(1, 2, 3), batch);
        assertEquals(0, queue.removeLast().size());
    }

    @Test
    void hasElementInAnyBatch() {
        BatchProcessingQueue<Integer> queue = new BatchProcessingQueue<>(3);
        queue.addToBatchToEnd(1);
        assertTrue(queue.hasElementInAnyBatch(1));
        assertFalse(queue.hasElementInAnyBatch(2));
    }

    @Test
    void softRemoveFromAllElementsRegistration() {
        BatchProcessingQueue<Integer> queue = new BatchProcessingQueue<>(3);
        queue.addToBatchToEnd(1);
        assertTrue(queue.hasElementInAnyBatch(1));
        queue.softRemoveFromAllElementsRegistration(1);
        assertFalse(queue.hasElementInAnyBatch(1));
    }

    @Test
    public void addSameElementTest(){
        BatchProcessingQueue<String> batchProcessingQueue = new BatchProcessingQueue<>(10);
        batchProcessingQueue.addToBatchToEnd("test1");
        batchProcessingQueue.addToBatchToEnd("test1");
        batchProcessingQueue.addToBatchToEnd("test1");
        Collection<String> elements = batchProcessingQueue.removeFirst();
        assertEquals(1, elements.size());
        assertTrue(elements.contains("test1"));
    }

    @Test
    public void removeFirstBatches(){
        BatchProcessingQueue<String> batchProcessingQueue = new BatchProcessingQueue<>(3);
        for(int i=0; i<8; i++){
            batchProcessingQueue.addToBatchToEnd("test"+i);
        }
        Collection<String> elements = batchProcessingQueue.removeFirst();
        assertEquals(3, elements.size());
        assertTrue(elements.contains("test0"));
        assertTrue(elements.contains("test1"));
        assertTrue(elements.contains("test2"));

        elements = batchProcessingQueue.removeFirst();
        assertEquals(3, elements.size());
        assertTrue(elements.contains("test3"));
        assertTrue(elements.contains("test4"));
        assertTrue(elements.contains("test5"));

        elements = batchProcessingQueue.removeFirst();
        assertEquals(2, elements.size());
        assertTrue(elements.contains("test6"));
        assertTrue(elements.contains("test7"));
    }

    @Test
    public void removeLastBatches(){
        BatchProcessingQueue<String> batchProcessingQueue = new BatchProcessingQueue<>(3);
        for(int i=0; i<8; i++){
            batchProcessingQueue.addToBatchToEnd("test"+i);
        }
        Collection<String> elements = batchProcessingQueue.removeLast();
        assertEquals(2, elements.size());
        assertTrue(elements.contains("test6"));
        assertTrue(elements.contains("test7"));

        elements = batchProcessingQueue.removeLast();
        assertEquals(3, elements.size());
        assertTrue(elements.contains("test3"));
        assertTrue(elements.contains("test4"));
        assertTrue(elements.contains("test5"));

        elements = batchProcessingQueue.removeLast();
        assertEquals(3, elements.size());
        assertTrue(elements.contains("test0"));
        assertTrue(elements.contains("test1"));
        assertTrue(elements.contains("test2"));
    }

    @Test
    public void softRemoveTest(){
        BatchProcessingQueue<String> batchProcessingQueue = new BatchProcessingQueue<>(10);
        batchProcessingQueue.addToBatchToEnd("test1");
        batchProcessingQueue.addToBatchToEnd("test2");
        batchProcessingQueue.addToBatchToEnd("test3");
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test1"));
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test2"));
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test3"));
        batchProcessingQueue.softRemoveFromAllElementsRegistration("test2");
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test1"));
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test2"));
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test3"));
        batchProcessingQueue.softRemoveFromAllElementsRegistration("test3");
        assertTrue(batchProcessingQueue.hasElementInAnyBatch("test1"));
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test2"));
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test3"));
        batchProcessingQueue.softRemoveFromAllElementsRegistration("test1");
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test1"));
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test2"));
        assertFalse(batchProcessingQueue.hasElementInAnyBatch("test3"));
        assertDoesNotThrow(() -> batchProcessingQueue.softRemoveFromAllElementsRegistration("notExistentElement"));
    }

}
