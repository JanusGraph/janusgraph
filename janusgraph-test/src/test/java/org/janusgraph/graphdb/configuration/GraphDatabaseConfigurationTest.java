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

package org.janusgraph.graphdb.configuration;

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.idmanagement.UniqueInstanceIdRetriever;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GraphDatabaseConfigurationTest {

    @Test
    public void testUniqueNames() {
        assertFalse(StringUtils.containsAny(UniqueInstanceIdRetriever.getInstance().getOrGenerateUniqueInstanceId(Configuration.EMPTY), ConfigElement.ILLEGAL_CHARS));
    }


}
