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

package org.janusgraph.graphdb.query.profile;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    public QueryProfiler addNested(String groupName, boolean hasSiblings) {
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
