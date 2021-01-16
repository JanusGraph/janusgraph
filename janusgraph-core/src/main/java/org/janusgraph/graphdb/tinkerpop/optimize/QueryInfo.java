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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder.OrderEntry;

import java.util.List;
import java.util.Objects;

/**
 *
 *  @author David Clement (david.clement90@laposte.net)
 *
 */
public class QueryInfo {

    private final  List<OrderEntry> orders;

    private int lowLimit;

    private int highLimit;

    public QueryInfo(List<OrderEntry> orders, int lowLimit, int highLimit) {
        this.orders = orders;
        this.lowLimit = lowLimit;
        this.highLimit = highLimit;
    }

    public List<OrderEntry> getOrders() {
        return orders;
    }

    public int getLowLimit() {
        return lowLimit;
    }

    public int getHighLimit() {
        return highLimit;
    }

    public QueryInfo setLowLimit(int lowLimit) {
        this.lowLimit = lowLimit;
        return this;
    }

    public QueryInfo setHighLimit(int highLimit) {
        this.highLimit = highLimit;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orders, lowLimit, highLimit);
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        QueryInfo oth = (QueryInfo)other;
        return Objects.equals(orders, oth.orders) && lowLimit == oth.lowLimit && highLimit == oth.highLimit;
    }
}
