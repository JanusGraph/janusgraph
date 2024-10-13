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
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreator;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonIndexDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJsonIndexCreator<T extends AbstractJsonIndexDefinition> implements JsonSchemaCreator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJsonIndexCreator.class);

    @Override
    public boolean create(T definition, JsonSchemaCreationContext context) {

        if (containsIndex(definition, context)) {
            LOG.info("Index with name [{}] was skipped because it already exists.", definition.getName());
            return false;
        }

        Index index = buildIndex(definition, context);
        LOG.info("Index {} was created", index.name());

        context.getCreatedOrUpdatedIndices().add(definition);

        return true;
    }

    protected abstract boolean containsIndex(T definition, JsonSchemaCreationContext context);

    protected abstract Index buildIndex(T definition, JsonSchemaCreationContext context);

}
