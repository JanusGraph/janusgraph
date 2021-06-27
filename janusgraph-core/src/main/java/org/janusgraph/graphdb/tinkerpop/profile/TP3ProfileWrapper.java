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

package org.janusgraph.graphdb.tinkerpop.profile;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TP3ProfileWrapper implements QueryProfiler {

    private final MutableMetrics metrics;
    private int subMetricCounter = 0;

    public TP3ProfileWrapper(MutableMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public QueryProfiler addNested(String groupName, boolean hasSiblings) {
        //Flatten out AND/OR nesting unless it has siblings
        if (!hasSiblings && (groupName.equals(AND_QUERY) || groupName.equals(OR_QUERY))) return this;

        int nextId = (subMetricCounter++);
        MutableMetrics nested = new MutableMetrics(metrics.getId()+"."+groupName+"_"+nextId,groupName);
        metrics.addNested(nested);
        return new TP3ProfileWrapper(nested);
    }

    public MutableMetrics getMetrics() {
        return metrics;
    }

    @Override
    public QueryProfiler setAnnotation(String key, Object value) {
        Preconditions.checkNotNull(key, "Key must be not null");
        Preconditions.checkNotNull(value, "Value must be not null");
        if (!(value instanceof String) && !(value instanceof Number)) value = value.toString();
        metrics.setAnnotation(key,value);
        return this;
    }

    @Override
    public void startTimer() {
        metrics.start();
    }

    @Override
    public void stopTimer() {
        metrics.stop();
    }

    @Override
    public void setResultSize(long size) {
        metrics.incrementCount(TraversalMetrics.ELEMENT_COUNT_ID,size);
    }
}
