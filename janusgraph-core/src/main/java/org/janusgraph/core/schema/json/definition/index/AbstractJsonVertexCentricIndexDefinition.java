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

package org.janusgraph.core.schema.json.definition.index;

import org.apache.tinkerpop.gremlin.process.traversal.Order;

import java.util.List;

public abstract class AbstractJsonVertexCentricIndexDefinition extends AbstractJsonIndexDefinition {

    private List<String> propertyKeys;
    private Order order;

    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }
}
