package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopElement implements Element, Comparable<HadoopElement> {

    static final Multimap<HadoopType, HadoopProperty> NO_PROPERTIES = ImmutableListMultimap.of();

    protected static final Predicate<HadoopProperty> FILTER_DELETED_PROPERTIES = new Predicate<HadoopProperty>() {
        @Override
        public boolean apply(@Nullable HadoopProperty p) {
            return !p.isDeleted();
        }
    };

    protected static final Predicate<HadoopEdge> FILTER_DELETED_EDGES = new Predicate<HadoopEdge>() {
        @Override
        public boolean apply(@Nullable HadoopEdge e) {
            return !e.isDeleted();
        }
    };

    protected long id;
    protected Multimap<HadoopType, HadoopProperty> properties = NO_PROPERTIES;
    protected ElementState state = ElementState.NEW;


    public HadoopElement(final long id) {
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

    void updateSchema(HadoopSerializer.Schema schema) {
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

    public boolean isModified() {
        for (final HadoopProperty property : this.getPropertiesWithState()) {
            if (property.isDeleted() || property.isNew())
                return true;
        }
        return false;
    }

    //##################################
    // Property Handling
    //##################################

    protected void initializeProperties() {
        if (this.properties == NO_PROPERTIES) properties = HashMultimap.create();
    }

    protected <T> T getImplicitProperty(final HadoopType type) {
        assert type.isImplicit();
        return null;
    }

    protected HadoopProperty addProperty(final HadoopProperty property) {
        Preconditions.checkNotNull(property);
        ElementHelper.validateProperty(this, property.getType().getName(), property.getValue());
        initializeProperties();
        if (properties.containsEntry(property.getType(), property)) {
            //Need to consolidate element states
            final HadoopProperty old = Iterables.getOnlyElement(Iterables.filter(properties.get(property.getType()), new Predicate<HadoopProperty>() {
                @Override
                public boolean apply(@Nullable HadoopProperty hadoopProperty) {
                    return hadoopProperty.equals(property);
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
        final HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        //Mark all existing ones for the type as deleted
        final Iterator<HadoopProperty> props = properties.get(type).iterator();
        while (props.hasNext()) {
            HadoopProperty p = props.next();
            if (p.isNew()) props.remove();
            else p.setState(ElementState.DELETED);
        }
        addProperty(new HadoopProperty(type, value));
    }

    @Override
    public <T> T removeProperty(final String key) {
        if (properties.isEmpty()) return null;
        final HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        final List<HadoopProperty> removed = Lists.newArrayList();
        final Iterator<HadoopProperty> props = properties.get(type).iterator();
        while (props.hasNext()) {
            HadoopProperty p = props.next();
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
        final HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return getImplicitProperty(type);

        Object result = null;
        for (final HadoopProperty p : properties.get(type)) {
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
        for (final HadoopProperty p : properties.values()) {
            if (!p.isDeleted() && !p.getType().isHidden()) result.add(p.getType().getName());
        }
        return result;
    }

    public Collection<HadoopProperty> getProperties() {
        final List<HadoopProperty> result = Lists.newArrayList(Iterables.filter(properties.values(), new Predicate<HadoopProperty>() {
            @Override
            public boolean apply(@Nullable HadoopProperty property) {
                return !property.getType().isHidden() && !property.isDeleted();
            }
        }));
        return result;
    }

    public void addAllProperties(final Iterable<HadoopProperty> properties) {
        for (final HadoopProperty p : properties) addProperty(p);
    }

    public Collection<HadoopProperty> getPropertiesWithState() {
        return this.properties.values();
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == ((HadoopElement) other).getIdAsLong();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    @Override
    public int compareTo(final HadoopElement other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }


}
