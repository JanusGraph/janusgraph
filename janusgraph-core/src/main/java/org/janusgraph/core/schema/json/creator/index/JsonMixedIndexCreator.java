// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema.json.creator.index;

import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.definition.index.JsonMixedIndexDefinition;

public class JsonMixedIndexCreator
        extends AbstractJsonGraphCentricIndexCreator<JsonMixedIndexDefinition> {

    @Override
    protected Index buildSpecificIndex(JanusGraphManagement graphManagement, JanusGraphManagement.IndexBuilder indexBuilder,
                                       JsonMixedIndexDefinition definition){

        return indexBuilder.buildMixedIndex(definition.getIndexBackend());
    }
}
