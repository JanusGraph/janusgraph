// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.builder;

import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyFunction;

import java.util.concurrent.ExecutorService;

public class CQLMutateManyFunctionWrapper {

    private final ExecutorService executorService;
    private final CQLMutateManyFunction mutateManyFunction;

    public CQLMutateManyFunctionWrapper(ExecutorService executorService, CQLMutateManyFunction mutateManyFunction) {
        this.executorService = executorService;
        this.mutateManyFunction = mutateManyFunction;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public CQLMutateManyFunction getMutateManyFunction() {
        return mutateManyFunction;
    }
}
