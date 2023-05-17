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

package org.janusgraph.graphdb.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

public class TestTraverserWithoutLoopsSupport<T> implements Traverser<T> {
    @Override
    public T get() {
        return null;
    }

    @Override
    public <S> S sack() {
        return null;
    }

    @Override
    public <S> void sack(S object) {

    }

    @Override
    public Path path() {
        return null;
    }

    @Override
    public int loops() {
        throw new IllegalStateException("This Test traverser doesn't support loops");
    }

    @Override
    public int loops(String loopName) {
        return loops();
    }

    @Override
    public long bulk() {
        return 0;
    }

    @Override
    public Traverser<T> clone() {
        return null;
    }
}
