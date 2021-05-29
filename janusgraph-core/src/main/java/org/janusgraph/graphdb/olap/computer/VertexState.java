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

package org.janusgraph.graphdb.olap.computer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexState<M> {

    protected Object properties;

    private Object previousMessages;
    private Object currentMessages;

    private VertexState() {
        properties=null;
        previousMessages=null;
        currentMessages=null;
    }

    public VertexState(Map<String,Integer> keyMap) {
        assert isValidIdMap(keyMap);
        previousMessages = null;
        currentMessages = null;

        if (keyMap.isEmpty() || keyMap.size()==1) properties = null;
        else properties = new Object[keyMap.size()];
    }

    public<V> void setProperty(String key, V value, Map<String,Integer> keyMap) {
        assert !keyMap.isEmpty() && keyMap.containsKey(key);
        if (keyMap.size()==1) properties = value;
        else ((Object[])properties)[keyMap.get(key)]=value;
    }

    public<V> V getProperty(String key, Map<String,Integer> keyMap) {
        assert !keyMap.isEmpty() && keyMap.containsKey(key);
        if (keyMap.size()==1) return (V)properties;
        else return (V)((Object[])properties)[keyMap.get(key)];
    }

    private void initializeCurrentMessages(Map<MessageScope,Integer> scopeMap) {
        assert !scopeMap.isEmpty() && isValidIdMap(scopeMap);
        if (currentMessages==null) {
            if (scopeMap.size()>1) currentMessages = new Object[scopeMap.size()];
        }
    }

    public synchronized void setMessage(M message, MessageScope scope, Map<MessageScope,Integer> scopeMap) {
        assert message!=null && scope!=null;
        Preconditions.checkArgument(scopeMap.containsKey(scope),"Provided scope was not declared in the VertexProgram: %s",scope);
        initializeCurrentMessages(scopeMap);
        if (scopeMap.size()==1) currentMessages = message;
        else ((Object[])currentMessages)[scopeMap.get(scope)]=message;
    }

    public synchronized void addMessage(M message, MessageScope scope, Map<MessageScope, Integer> scopeMap,
                                        MessageCombiner<M> combiner) {
        assert message != null && scope != null;
        Preconditions.checkArgument(scopeMap.containsKey(scope), "Provided scope was not declared in the VertexProgram: %s", scope);
        initializeCurrentMessages(scopeMap);
        if (scopeMap.size() == 1) {
            currentMessages = currentMessages == null ? message : combineMessages(combiner, message, currentMessages);
        } else {
            int pos = scopeMap.get(scope);
            Object[] messages = (Object[]) currentMessages;
            messages[pos] = messages[pos] == null ? message : combineMessages(combiner, message, messages[pos]);
        }
    }

    private Object combineMessages(MessageCombiner<M> combiner, M messageA, Object messageB) {
        Object combinedMessage;
        if (combiner != null) {
            combinedMessage = combiner.combine(messageA, (M) messageB);
        } else {
            if (messageB instanceof ObjectArrayList) {
                ((ObjectArrayList) messageB).add(messageA);
                combinedMessage = messageB;
            } else {
                ObjectArrayList<Object> arrayList = new ObjectArrayList<>();
                arrayList.add(messageA, messageB);
                combinedMessage = arrayList;
            }
        }
        return combinedMessage;
    }

    public Stream<M> getMessage(MessageScope scope, Map<MessageScope, Integer> scopeMap) {
        assert scope != null && isValidIdMap(scopeMap) && scopeMap.containsKey(scope);
        Object message;
        if (scopeMap.size() == 1) {
            message = previousMessages;
        } else if (previousMessages != null) {
            message = ((Object[]) previousMessages)[scopeMap.get(scope)];
        } else {
            message = null;
        }
        if (message instanceof ObjectArrayList) {
            Iterator<M> transform = Iterators.transform(((ObjectArrayList<M>) message).iterator(), e -> e.value);
            return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(transform, Spliterator.IMMUTABLE | Spliterator.SIZED), false);
        } else {
            return message == null ? Stream.empty() : Stream.of((M) message);
        }
    }

    public synchronized void completeIteration() {
        previousMessages = currentMessages;
        currentMessages = null;
    }

    public static boolean isValidIdMap(Map<?,Integer> map) {
        if (map==null) return false;
        if (map.isEmpty()) return true;
        int size = map.size();
        Set<Integer> ids = new HashSet<>(size);
        for (Integer id : map.values()) {
            if (id>=size || id<0) return false;
            if (!ids.add(id)) return false;
        }
        return true;
    }

    static final VertexState EMPTY_STATE = new EmptyState();

    private static class EmptyState<M> extends VertexState<M> {

        @Override
        public<V> void setProperty(String key, V value, Map<String,Integer> keyMap) {
            throw new UnsupportedOperationException();
        }

        @Override
        public<V> V getProperty(String key, Map<String,Integer> keyMap) {
            return null;
        }

        @Override
        public Stream<M> getMessage(MessageScope scope, Map<MessageScope,Integer> scopeMap) {
            return Stream.empty();
        }

        @Override
        public synchronized void setMessage(M message, MessageScope scope, Map<MessageScope,Integer> scopeMap) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void addMessage(M message, MessageScope scope, Map<MessageScope,Integer> scopeMap,
                                            MessageCombiner<M> combiner) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void completeIteration() {
            throw new UnsupportedOperationException();
        }
    }
}
