// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.plugin;

import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphGremlinDriverPluginTest {
    @Test
    public void shouldImportDeclaringClass() {
        final JanusGraphGremlinDriverPlugin plugin = JanusGraphGremlinDriverPlugin.instance();
        final ImportCustomizer customizer = (ImportCustomizer) plugin.getCustomizers().get()[0];
        final Set<Class> classes = customizer.getClassImports();

        for (final Method m : customizer.getMethodImports()) {
            final Class c = m.getDeclaringClass();
            assertTrue(classes.contains(c), "CLASS_IMPORTS is missing " + c.getCanonicalName());
        }

        for (final Enum e : customizer.getEnumImports()) {
            final Class c = e.getDeclaringClass();
            assertTrue(classes.contains(c), "CLASS_IMPORTS is missing " + c.getCanonicalName());
        }
    }
}
