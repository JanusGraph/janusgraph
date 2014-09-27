package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardFaunusProperty extends StandardFaunusRelation implements FaunusProperty {

    protected long vertexid;
    protected Object value;

    private static final Logger log =
            LoggerFactory.getLogger(StandardFaunusProperty.class);

    public StandardFaunusProperty() {
        this(ModifiableHadoopConfiguration.immutableWithResources());
    }

    public StandardFaunusProperty(final Configuration configuration) {
        super(configuration, FaunusElement.NO_ID, FaunusPropertyKey.VALUE);
    }

    public StandardFaunusProperty(final Configuration configuration, final DataInput in) throws IOException {
        super(configuration, FaunusElement.NO_ID, FaunusPropertyKey.VALUE);
        this.readFields(in);
    }

    public StandardFaunusProperty(FaunusVertex vertex, FaunusPropertyKey type, Object value) {
        this(FaunusElement.NO_ID, vertex, type, value);
    }

    public StandardFaunusProperty(long id, FaunusVertex vertex, String type, Object value) {
        this(id, vertex, vertex.getTypeManager().getOrCreatePropertyKey(type), value);
    }

    public StandardFaunusProperty(long id, FaunusVertex vertex, FaunusPropertyKey type, Object value) {
        this(vertex.getFaunusConf(),id,vertex.getLongId(),type,value);
    }

    public StandardFaunusProperty(Configuration config, long id, long vertex, FaunusPropertyKey type, Object value) {
        super(config, id, type);
        Preconditions.checkArgument(vertex>=0, "Vertex id %d", vertex);
        Preconditions.checkNotNull(value, "property value must be non-null");
        Preconditions.checkArgument(!type.isImplicit(),"Cannot set implicit properties: " + type);
        Preconditions.checkArgument(AttributeUtil.hasGenericDataType(type) ||
                type.getDataType().isInstance(value),"Value does not match data type: %s",value);
        this.value = value;
        this.vertexid = vertex;
        log.debug("Initialized property {}", this);
    }

    public Object getValue() {
        return value;
    }

    @Override
    public PropertyKey getPropertyKey() {
        return (PropertyKey)getType();
    }

    @Override
    public TitanVertex getVertex() {
        return getVertex(0);
    }

    @Override
    public TitanVertex getVertex(int pos) {
        Preconditions.checkArgument(pos==0,"Invalid position: %s",pos);
        return new FaunusVertex(getFaunusConf(), vertexid);
    }

    final void setKey(FaunusPropertyKey key) {
        Preconditions.checkNotNull(key);
        setType(key);
    }


    //##################################
    // Serialization Proxy
    //##################################

    @Override
    public void write(final DataOutput out) throws IOException {
        new FaunusSerializer(getFaunusConf()).writeProperty(this, out);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        new FaunusSerializer(getFaunusConf()).readProperty(this, in);

    }

    //##################################
    // General Utility
    //##################################


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(vertexid).append(getLongId()).append(getType()).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth == null || !(oth instanceof TitanProperty)) return false;
        TitanProperty p = (TitanProperty) oth;
        if (hasId() || p.hasId()) return getLongId()==p.getLongId();
        return getType().equals(p.getPropertyKey()) && value.equals(p.getValue()) && vertexid==p.getVertex().getLongId();
    }

    @Override
    public String toString() {
        return getTypeName() + "->" + (null != value ? value.toString() : null);
    }
}
