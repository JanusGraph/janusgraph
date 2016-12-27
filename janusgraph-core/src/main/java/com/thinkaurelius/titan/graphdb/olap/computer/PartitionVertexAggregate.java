package com.thinkaurelius.titan.graphdb.olap.computer;

import com.thinkaurelius.titan.diskstorage.EntryList;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;

import java.util.Collections;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionVertexAggregate<M> extends VertexState<M> {

    public PartitionVertexAggregate(Map<MessageScope,Integer> scopeMap) {
        super(Collections.EMPTY_MAP);

    }

    public synchronized void setLoadedProperties(EntryList props) {
        assert properties==null;
        properties = props;
    }

    public EntryList getLoadedProperties() {
        return (EntryList)properties;
    }

    public<V> void setProperty(String key, V value, Map<String,Integer> keyMap) {
        throw new UnsupportedOperationException();
    }

    public<V> V getProperty(String key, Map<String,Integer> keyMap) {
        throw new UnsupportedOperationException();
    }



}
