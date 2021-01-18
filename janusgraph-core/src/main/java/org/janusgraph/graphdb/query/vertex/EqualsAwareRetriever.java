// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.query.vertex;

import org.janusgraph.util.datastructures.Retriever;

public class EqualsAwareRetriever<I, O, E> implements Retriever<I, O> {

    private final E id;
    private final Retriever<I, O> retriever;

    public EqualsAwareRetriever(E id, Retriever<I, O> retriever) {
        this.id = id;
        this.retriever = retriever;
    }

    @Override
    public O get(I input) {
        return retriever.get(input);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof EqualsAwareRetriever))
            return false;

        EqualsAwareRetriever<?, ?, ?> oth = (EqualsAwareRetriever) other;
        return id.equals(oth.id);
    }
}
