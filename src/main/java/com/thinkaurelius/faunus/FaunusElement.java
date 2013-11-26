package com.thinkaurelius.faunus;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
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
public abstract class FaunusElement implements Element, WritableComparable<FaunusElement> {

    static final ListMultimap<FaunusType,Object> NO_PROPERTIES = ImmutableListMultimap.of();

    protected long id;
    protected ListMultimap<FaunusType, Object> properties = NO_PROPERTIES;
    protected List<List<MicroElement>> paths = null;
    protected MicroElement microVersion = null;
    protected boolean pathEnabled = false;
    protected long pathCounter = 0;

    public FaunusElement(final long id) {
        this.id = id;
    }

    protected FaunusElement reuse(final long id) {
        this.id = id;
        this.properties = null;
        this.clearPaths();
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

    //##################################
    // Property Handling
    //##################################

    @Override
    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        FaunusType type = FaunusGraph.getCurrent().getTypes().get(key);
        if (properties == NO_PROPERTIES)
            this.properties = ArrayListMultimap.create();
        this.properties.removeAll(type);
        this.properties.put(type, value);
    }

    @Override
    public <T> T removeProperty(final String key) {
        if (properties.isEmpty()) return null;
        Collection<Object>  removed = this.properties.removeAll(FaunusGraph.getCurrent().getTypes().get(key));
        if (removed.isEmpty()) return null;
        else if (removed.size()==1) return (T)removed.iterator().next();
        else return (T)removed;
    }

    @Override
    public <T> T getProperty(final String key) {
        FaunusType type = FaunusGraph.getCurrent().getTypes().get(key);
        //First, handle special cases
        if (type.equals(FaunusType.COUNT))
            return (T) Long.valueOf(this.pathCount());

        Collection<Object> values = properties.get(type);
        if (values.isEmpty()) return null;
        else if (values.size()==1) return (T)values.iterator().next();
        else throw new IllegalStateException("Use getProperties(String) method for multi-valued properties");
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> result = Sets.newHashSet();
        for (FaunusType type : properties.keySet()) result.add(type.getName());
        return result;
    }

    public Collection<FaunusProperty> getProperties() {
        List<FaunusProperty> result = Lists.newArrayList();
        for (Map.Entry<FaunusType,Object> entry : properties.entries()) {
            result.add(new FaunusProperty(entry.getKey(),entry.getValue()));
        }
        return result;
    }

    public void addAllProperties(Iterable<FaunusProperty> properties) {
        for (FaunusProperty p : properties) {
            this.properties.put(p.getType(),p.getValue());
        }
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

    //##################################
    // Path Handling
    //##################################

    public void enablePath(final boolean enablePath) {
        this.pathEnabled = enablePath;
        if (this.pathEnabled) {
            if (null == this.microVersion)
                this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
            if (null == this.paths)
                this.paths = new ArrayList<List<MicroElement>>();
        }
        // TODO: else make pathCounter = paths.size()?
    }

    public void addPath(final List<MicroElement> path, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) path.add(this.microVersion);
            this.paths.add(path);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public void addPaths(final List<List<MicroElement>> paths, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) {
                for (final List<MicroElement> path : paths) {
                    this.addPath(path, append);
                }
            } else
                this.paths.addAll(paths);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public List<List<MicroElement>> getPaths() throws IllegalStateException {
        if (this.pathEnabled)
            return this.paths;
        else
            throw new IllegalStateException("Path calculations are not enabled");
    }

    public void getPaths(final FaunusElement element, final boolean append) {
        if (this.pathEnabled) {
            this.addPaths(element.getPaths(), append);
        } else {
            this.pathCounter = this.pathCounter + element.pathCount();
        }
    }

    public long incrPath(final long amount) throws IllegalStateException {
        if (this.pathEnabled)
            throw new IllegalStateException("Path calculations are enabled -- use addPath()");
        else
            this.pathCounter = this.pathCounter + amount;
        return this.pathCounter;
    }

    public boolean hasPaths() {
        if (this.pathEnabled)
            return !this.paths.isEmpty();
        else
            return this.pathCounter > 0;
    }

    public void clearPaths() {
        if (this.pathEnabled) {
            this.paths = new ArrayList<List<MicroElement>>();
            this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
        } else
            this.pathCounter = 0;
    }

    public long pathCount() {
        if (this.pathEnabled)
            return this.paths.size();
        else
            return this.pathCounter;
    }

    public void startPath() {
        if (this.pathEnabled) {
            this.clearPaths();
            final List<MicroElement> startPath = new ArrayList<MicroElement>();
            startPath.add(this.microVersion);
            this.paths.add(startPath);
        } else {
            this.pathCounter = 1;
        }
    }



    public static abstract class MicroElement {

        protected final long id;

        public MicroElement(final long id) {
            this.id = id;
        }

        public long getId() {
            return this.id;
        }

        @Override
        public int hashCode() {
            return Long.valueOf(this.id).hashCode();
        }

        @Override
        public boolean equals(final Object object) {
            return (object.getClass().equals(this.getClass()) && this.id == ((MicroElement) object).getId());
        }
    }
}
