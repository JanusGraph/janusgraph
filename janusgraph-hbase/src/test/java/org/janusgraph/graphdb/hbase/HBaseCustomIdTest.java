// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.hbase;

import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.JanusGraphCustomIdTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class HBaseCustomIdTest extends JanusGraphCustomIdTest {
    @Container
    public static final HBaseContainer hBaseContainer = new HBaseContainer();

    @Override
    public ModifiableConfiguration getModifiableConfiguration() {
        return hBaseContainer.getModifiableConfiguration();
    }
}
