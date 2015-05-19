package com.thinkaurelius.titan.graphdb.tinkerpop.profile;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;

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
    public QueryProfiler addNested(String groupName) {
        //Flatten out AND/OR nesting
        if (groupName.equals(AND_QUERY) || groupName.equals(OR_QUERY)) return this;

        int nextId = (subMetricCounter++);
        MutableMetrics nested = new MutableMetrics(metrics.getId()+"."+groupName+"_"+nextId,groupName);
        metrics.addNested(nested);
        return new TP3ProfileWrapper(nested);
    }

    @Override
    public QueryProfiler setAnnotation(String key, Object value) {
        Preconditions.checkArgument(key != null && value != null, "Key and value must be not null");
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
