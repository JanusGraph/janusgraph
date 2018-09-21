// Copyright 2018 JanusGraph Authors
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

package org.janusgraph;

import org.janusgraph.blueprints.JanusGraphComputerTest;
import org.janusgraph.blueprints.JanusGraphProcessTest;
import org.janusgraph.blueprints.JanusGraphStructureTest;
import org.janusgraph.blueprints.MultiQueryJanusGraphProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class JanusGraphTinkerpopTestSuite extends AbstractJanusGraphTestSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
        JanusGraphComputerTest.class,
        JanusGraphProcessTest.class,
        JanusGraphStructureTest.class,
        MultiQueryJanusGraphProcessTest.class,
    };

    public JanusGraphTinkerpopTestSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests);
    }
}
