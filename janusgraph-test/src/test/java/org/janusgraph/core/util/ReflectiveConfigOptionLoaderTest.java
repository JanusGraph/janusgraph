// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.core.util;

import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.junit.jupiter.api.Test;
import org.reflections8.Reflections;
import org.reflections8.scanners.SubTypesScanner;
import org.reflections8.scanners.TypeAnnotationsScanner;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ReflectiveConfigOptionLoaderTest {

    /**
     * The test checks if the included `reflections` dependency with the bug
     * https://github.com/ronmamo/reflections/issues/273 or not.
     * The bug in `reflections` was introduced in the version `0.9.12`.
     */
    @Test
    public void shouldLoadAllClasses(){
        org.reflections8.Configuration rc = new org.reflections8.util.ConfigurationBuilder()
            .setUrls(Collections.emptyList())
            .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner());
        Reflections reflections = new Reflections(rc);
        assertDoesNotThrow(() -> reflections.getTypesAnnotatedWith(PreInitializeConfigOptions.class));
        /*
        To reproduce the problem in the code, we can use the next example but notice that the assertion will never fail
        because we swallow any errors reproduced in `loadAll`. That is why, the above code mimics the below configuration.

        ReflectiveConfigOptionLoader.INSTANCE.setUseThreadContextLoader(false);
        ReflectiveConfigOptionLoader.INSTANCE.setUseCallerLoader(false);
        ReflectiveConfigOptionLoader.INSTANCE.setPreferredClassLoaders(Collections.emptyList());
        Assertions.assertDoesNotThrow(() -> ReflectiveConfigOptionLoader.INSTANCE.loadAll(this.getClass()));
         */
    }
}
