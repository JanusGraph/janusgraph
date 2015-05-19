package com.thinkaurelius.titan.graphdb.query.profile;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleQueryProfiler implements QueryProfiler, Iterable<SimpleQueryProfiler> {

    private final List<SimpleQueryProfiler> nestedProfilers = new ArrayList<>();
    private final Map<String,Object> annotations = new HashMap<>();

    private final String groupName;
    private long resultSize = 0;

    private long startTimeNs = 0;
    private boolean runningTimer = false;
    private long measuredTimeNs = 0;

    public SimpleQueryProfiler() {
        this("__root");
    }

    public SimpleQueryProfiler(final String groupName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(groupName));
        this.groupName=groupName;
    }

    @Override
    public QueryProfiler addNested(String groupName) {
        SimpleQueryProfiler nested = new SimpleQueryProfiler(groupName);
        nestedProfilers.add(nested);
        return nested;
    }

    public String getGroupName() {
        return groupName;
    }

    @Override
    public QueryProfiler setAnnotation(String key, Object value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "Must provide a key");
        annotations.put(key,convert(value));
        return this;
    }

    private Object convert(Object value) {
        Preconditions.checkArgument(value != null, "Value may not be null");
//        if (value instanceof CharSequence) return value.toString();
//        if (value instanceof Number || value instanceof Boolean) return value;
//        if (value.getClass().isEnum()) return value;
//        return value.toString();
        return value;
    }

    @Override
    public void startTimer() {
        Preconditions.checkArgument(!runningTimer,"A timer is already running");
        startTimeNs = System.nanoTime();
        runningTimer = true;
    }

    @Override
    public void stopTimer() {
        Preconditions.checkArgument(runningTimer,"No timer running");
        measuredTimeNs+=(System.nanoTime()-startTimeNs);
        runningTimer=false;
    }

    @Override
    public void setResultSize(long size) {
        Preconditions.checkArgument(size>=0);
        this.resultSize=size;
    }

    //RETRIEVAL METHODS

    @Override
    public Iterator<SimpleQueryProfiler> iterator() {
        return nestedProfilers.iterator();
    }

    public<O> O getAnnotation(String key) {
        return (O)annotations.get(key);
    }

    public Map<String,Object> getAnnotations() {
        return annotations;
    }

    public long getTotalTime() {
        return measuredTimeNs;
    }

    public long getResultSize() {
        return resultSize;
    }

}
