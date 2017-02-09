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

package org.janusgraph.core;

// TODO is this vestigial now that TP3's VertexProperty.Cardinality exists?

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * The cardinality of the values associated with given key for a particular element.
 *
* @author Matthias Broecheler (me@matthiasb.com)
*/
public enum Cardinality {

    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE,

    /**
     * Multiple values and duplicate values may be associated with the given key.
     */
    LIST,


    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET;

    public org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality convert() {
        switch (this) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new AssertionError("Unrecognized cardinality: " + this);
        }
    }

    public static Cardinality convert(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single: return SINGLE;
            case list: return LIST;
            case set: return SET;
            default: throw new AssertionError("Unrecognized cardinality: " + cardinality);
        }
    }


}
