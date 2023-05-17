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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Class helper to batch elements in the Queue for processing.
 *
 * @param <T> Elements to batch
 */
public class BatchProcessingQueue<T> {

    private final Set<T> allBatchedElements = new HashSet<>();
    private final Queue<List<T>> batches = new ArrayDeque<>(2);
    private List<T> lastBatch = new ArrayList<>();

    private int batchSize;

    public BatchProcessingQueue(int batchSize) {
        this.batchSize = batchSize;
    }

    public void addToBatch(T element) {
        if(!allBatchedElements.add(element)){
            return;
        }
        if(lastBatch.size() >= batchSize){
            batches.add(lastBatch);
            lastBatch = new ArrayList<>();
        }
        lastBatch.add(element);
    }

    public Collection<T> pollBatch(){
        final List<T> batch;
        if(batches.isEmpty()){
            batch = lastBatch;
            lastBatch = new ArrayList<>();
        } else {
            batch = batches.remove();
        }
        allBatchedElements.removeAll(batch);
        return batch;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
