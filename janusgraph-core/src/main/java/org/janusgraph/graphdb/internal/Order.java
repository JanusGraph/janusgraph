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

package org.janusgraph.graphdb.internal;

/**
 * Constants to specify the ordering of a result set in queries.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum Order {

    /**
     * Increasing
     */
    ASC,
    /**
     * Decreasing
     */
    DESC;

    /**
     * Modulates the result of a {@link Comparable#compareTo(Object)} execution for this specific
     * order, i.e. it negates the result if the order is {@link #DESC}.
     *
     * @param compare
     * @return
     */
    public int modulateNaturalOrder(int compare) {
        switch (this) {
            case ASC:
                return compare;
            case DESC:
                return -compare;
            default:
                throw new AssertionError("Unrecognized order: " + this);
        }
    }

    /**
     * The default order when none is specified
     */
    public static final Order DEFAULT = ASC;

    public org.apache.tinkerpop.gremlin.process.traversal.Order getTP() {
        switch (this) {
            case ASC :return org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
            case DESC: return org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
            default: throw new AssertionError();
        }
    }

    public static Order convert(org.apache.tinkerpop.gremlin.process.traversal.Order order) {
        switch(order) {
            case asc: return ASC;
            case desc: return DESC;
            default: throw new AssertionError();
        }
    }

}
