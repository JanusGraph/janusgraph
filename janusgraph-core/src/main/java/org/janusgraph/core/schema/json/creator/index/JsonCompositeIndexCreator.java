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

import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.definition.index.JsonCompositeIndexDefinition;

public class JsonCompositeIndexCreator extends AbstractJsonGraphCentricIndexCreator<JsonCompositeIndexDefinition> {

    @Override
    protected Index buildSpecificIndex(JanusGraphManagement graphManagement, JanusGraphManagement.IndexBuilder indexBuilder,
                                       JsonCompositeIndexDefinition definition){
        if(Boolean.TRUE.equals(definition.getUnique())){
            indexBuilder.unique();
        }

        if(CollectionUtils.isNotEmpty(definition.getInlinePropertyKeys())){
            for(String inlinePropertyKey : definition.getInlinePropertyKeys()){
                PropertyKey propertyKey = graphManagement.getPropertyKey(inlinePropertyKey);
                indexBuilder.addInlinePropertyKey(propertyKey);
            }
        }

        JanusGraphIndex index = indexBuilder.buildCompositeIndex();

        if(definition.getConsistency() != null){
            graphManagement.setConsistency(index, definition.getConsistency());
        }

        return index;
    }
}
