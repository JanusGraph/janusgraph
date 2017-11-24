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

package org.janusgraph.graphdb.types;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.SchemaStatus;

/**
* @author Matthias Broecheler (me@matthiasb.com)
*/
public interface CompositeIndexType extends IndexType {

    long getID();

    IndexField[] getFieldKeys();

    SchemaStatus getStatus();

    /*
     * single == unique,
     */
    Cardinality getCardinality();

    ConsistencyModifier getConsistencyModifier();
}
