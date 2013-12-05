package com.thinkaurelius.faunus;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.ReadByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement implements Element, Comparable<FaunusElement> {

    static final Multimap<FaunusType,FaunusProperty> NO_PROPERTIES = ImmutableListMultimap.of();

    protected static final Predicate<FaunusProperty> FILTER_DELETED_PROPERTIES = new Predicate<FaunusProperty>() {
        @Override
        public boolean apply(@Nullable FaunusProperty p) {
            return !p.isDeleted();
        }
    };

    protected static final Predicate<FaunusEdge> FILTER_DELETED_EDGES = new Predicate<FaunusEdge>() {
        @Override
        public boolean apply(@Nullable FaunusEdge e) {
            return !e.isDeleted();
        }
    };

    protected long id;
    protected Multimap<FaunusType, FaunusProperty> properties = NO_PROPERTIES;
    protected ElementState state = ElementState.LOADED;


    public FaunusElement(final long id) {
        this.id = id;
    }

    protected FaunusElement reuse(final long id) {
        this.id = id;
        this.properties = NO_PROPERTIES;
        return this;
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        //TODO: should this be supported?
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getId() {
        return this.id;
    }

    public long getIdAsLong() {
        return this.id;
    }

    void updateSchema(FaunusSerializer.Schema schema) {
        schema.addAll(properties.keySet());
    }

    void setState(ElementState state) {
        Preconditions.checkNotNull(state);
        this.state=state;
    }

    public ElementState getState() {
        return state;
    }

    public boolean isNew() {
        return state==ElementState.NEW;
    }

    public boolean isDeleted() {
        return state==ElementState.DELETED;
    }

    public boolean isModified() {
        return state!=ElementState.LOADED;
    }

    //##################################
    // Property Handling
    //##################################

    protected void initializeProperties() {
        if (properties==NO_PROPERTIES) properties = HashMultimap.create();
    }

    protected void addProperty(final FaunusProperty property) {
        Preconditions.checkNotNull(property);
        ElementHelper.validateProperty(this, property.getType().getName(), property.getValue());
        //TODO: property.setState(ElementState.NEW) ???
        initializeProperties();
        this.properties.put(property.getType(),property);
    }

    protected <T> T getImplicitProperty(final FaunusType type) {
        assert type.isImplicit();
        return null;
    }

    @Override
    public void setProperty(final String key, final Object value) {
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        if (!this.properties.isEmpty()) this.properties.removeAll(type);
        addProperty(new FaunusProperty(type, value));
    }

    @Override
    public <T> T removeProperty(final String key) {
        if (properties.isEmpty()) return null;
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        List<FaunusProperty>  removed = Lists.newArrayList();
        for (FaunusProperty p : properties.get(type)) {
            if (p.isDeleted()) continue;
            p.setState(ElementState.DELETED);
            removed.add(p);
        }
        properties.removeAll(type);
        if (removed.isEmpty()) return null;
        else if (removed.size()==1) return (T)removed.iterator().next().getValue();
        else return (T)removed;
    }


    @Override
    public <T> T getProperty(final String key) {
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return getImplicitProperty(type);

        Object result = null;
        for (FaunusProperty p : properties.get(type)) {
            if (p.isDeleted()) continue;
            if (result!=null) throw new IllegalStateException("Use getProperties(String) method for multi-valued properties");
            result = p.getValue();
        }
        return (T)result;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> result = Sets.newHashSet();
        for (FaunusProperty p : properties.values()) {
            if (!p.isDeleted() && !p.getType().isHidden()) result.add(p.getType().getName());
        }
        return result;
    }

    public Collection<FaunusProperty> getProperties() {
        List<FaunusProperty> result = Lists.newArrayList(Iterables.filter(properties.values(),new Predicate<FaunusProperty>() {
            @Override
            public boolean apply(@Nullable FaunusProperty property) {
                return !property.getType().isHidden() && !property.isDeleted();
            }
        }));
        return result;
    }

    public Collection<FaunusProperty> getAllProperties() {
        return properties.values();
    }

    public void addAllProperties(Iterable<FaunusProperty> properties) {
        //TODO: Should those be marked as NEW?
        for (FaunusProperty p : properties) addProperty(p);
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == ((FaunusElement) other).getIdAsLong();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    @Override
    public int compareTo(final FaunusElement other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }


}
