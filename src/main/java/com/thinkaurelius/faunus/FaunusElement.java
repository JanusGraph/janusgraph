package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement implements Element, Comparable<FaunusElement> {

    static final Multimap<FaunusType, FaunusProperty> NO_PROPERTIES = ImmutableListMultimap.of();

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
    protected ElementState state = ElementState.NEW;


    public FaunusElement(final long id) {
        this.id = id;
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

    public void setId(final long id) {
        this.id = id;
    }

    void updateSchema(FaunusSerializer.Schema schema) {
        schema.addAll(properties.keySet());
    }

    public void setState(ElementState state) {
        Preconditions.checkNotNull(state);
        this.state = state;
    }

    public ElementState getState() {
        return this.state;
    }

    public boolean isNew() {
        return this.state == ElementState.NEW;
    }

    public boolean isDeleted() {
        return this.state == ElementState.DELETED;
    }

    public boolean isLoaded() {
        return this.state == ElementState.LOADED;
    }

    //##################################
    // Property Handling
    //##################################

    protected void initializeProperties() {
        if (this.properties == NO_PROPERTIES) properties = HashMultimap.create();
    }

    protected <T> T getImplicitProperty(final FaunusType type) {
        assert type.isImplicit();
        return null;
    }

    protected FaunusProperty addProperty(final FaunusProperty property) {
        Preconditions.checkNotNull(property);
        ElementHelper.validateProperty(this, property.getType().getName(), property.getValue());
        initializeProperties();
        if (properties.containsEntry(property.getType(), property)) {
            //Need to consolidate element states
            final FaunusProperty old = Iterables.getOnlyElement(Iterables.filter(properties.get(property.getType()), new Predicate<FaunusProperty>() {
                @Override
                public boolean apply(@Nullable FaunusProperty faunusProperty) {
                    return faunusProperty.equals(property);
                }
            }));
            if (property.isNew() && old.isDeleted()) {
                old.setState(ElementState.LOADED);
            } else if (property.isLoaded() && old.isNew()) {
                old.setState(ElementState.LOADED);
            }
            return old;
        } else {
            properties.put(property.getType(), property);
            return property;
        }
    }

    @Override
    public void setProperty(final String key, final Object value) {
        final FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        //Mark all existing ones for the type as deleted
        final Iterator<FaunusProperty> props = properties.get(type).iterator();
        while (props.hasNext()) {
            FaunusProperty p = props.next();
            if (p.isNew()) props.remove();
            else p.setState(ElementState.DELETED);
        }
        addProperty(new FaunusProperty(type, value));
    }

    @Override
    public <T> T removeProperty(final String key) {
        if (properties.isEmpty()) return null;
        final FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        final List<FaunusProperty> removed = Lists.newArrayList();
        final Iterator<FaunusProperty> props = properties.get(type).iterator();
        while (props.hasNext()) {
            FaunusProperty p = props.next();
            if (!p.isDeleted()) removed.add(p);
            if (p.isNew()) props.remove();
            else p.setState(ElementState.DELETED);
        }
        if (removed.isEmpty()) return null;
        else if (removed.size() == 1) return (T) removed.iterator().next().getValue();
        else return (T) removed;
    }


    @Override
    public <T> T getProperty(final String key) {
        final FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return getImplicitProperty(type);

        Object result = null;
        for (FaunusProperty p : properties.get(type)) {
            if (p.isDeleted()) continue;
            if (result != null)
                throw new IllegalStateException("Use getProperties(String) method for multi-valued properties");
            result = p.getValue();
        }
        return (T) result;
    }

    @Override
    public Set<String> getPropertyKeys() {
        final Set<String> result = Sets.newHashSet();
        for (final FaunusProperty p : properties.values()) {
            if (!p.isDeleted() && !p.getType().isHidden()) result.add(p.getType().getName());
        }
        return result;
    }

    public Collection<FaunusProperty> getProperties() {
        final List<FaunusProperty> result = Lists.newArrayList(Iterables.filter(properties.values(), new Predicate<FaunusProperty>() {
            @Override
            public boolean apply(@Nullable FaunusProperty property) {
                return !property.getType().isHidden() && !property.isDeleted();
            }
        }));
        return result;
    }

    public void addAllProperties(final Iterable<FaunusProperty> properties) {
        for (final FaunusProperty p : properties) addProperty(p);
    }

    public Collection<FaunusProperty> getPropertiesWithState() {
        return this.properties.values();
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
