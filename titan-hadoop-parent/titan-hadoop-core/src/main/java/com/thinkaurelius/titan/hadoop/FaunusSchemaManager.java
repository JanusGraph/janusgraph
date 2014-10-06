package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.schema.SchemaInspector;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.schema.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.schema.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.schema.RelationTypeDefinition;
import com.thinkaurelius.titan.graphdb.schema.SchemaProvider;

import java.util.concurrent.ConcurrentMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusSchemaManager implements SchemaInspector {

    private static final FaunusSchemaManager DEFAULT_MANAGER = new FaunusSchemaManager();

    private final ConcurrentMap<String,FaunusVertexLabel> vertexLabels;
    private final ConcurrentMap<String,FaunusRelationType> relationTypes;
    private SchemaProvider schemaProvider;

    private FaunusSchemaManager() {
        this(DefaultSchemaProvider.INSTANCE);
    }

    public FaunusSchemaManager(SchemaProvider provider) {
        vertexLabels = Maps.newConcurrentMap();
        relationTypes = Maps.newConcurrentMap();
        setSchemaProvider(provider);
        initialize();
    }

    private final void initialize() {
        vertexLabels.put(FaunusVertexLabel.DEFAULT_VERTEXLABEL.getName(),FaunusVertexLabel.DEFAULT_VERTEXLABEL);
        relationTypes.put(FaunusPropertyKey.COUNT.getName(),FaunusPropertyKey.COUNT);
        relationTypes.put(FaunusEdgeLabel.LINK.getName(),FaunusEdgeLabel.LINK);
        relationTypes.put(FaunusPropertyKey.VALUE.getName(),FaunusPropertyKey.VALUE);
        relationTypes.put(FaunusPropertyKey.ID.getName(),FaunusPropertyKey.ID);
        relationTypes.put(FaunusPropertyKey._ID.getName(),FaunusPropertyKey._ID);
        relationTypes.put(FaunusPropertyKey.LABEL.getName(),FaunusPropertyKey.LABEL);
    }

    public void setSchemaProvider(SchemaProvider provider) {
        if (provider!=DefaultSchemaProvider.INSTANCE) {
            provider = DefaultSchemaProvider.asBackupProvider(provider);
        }
        this.schemaProvider=provider;
    }

    public void clear() {
        vertexLabels.clear();
        relationTypes.clear();
        initialize();
    }

    public FaunusVertexLabel getVertexLabel(String name) {
        FaunusVertexLabel vl = vertexLabels.get(name);
        if (vl==null) {
            vertexLabels.putIfAbsent(name,new FaunusVertexLabel(schemaProvider.getVertexLabel(name)));
            vl = vertexLabels.get(name);
        }
        assert vl!=null;
        return vl;
    }


    @Override
    public boolean containsVertexLabel(String name) {
        return vertexLabels.containsKey(name) || schemaProvider.getVertexLabel(name)!=null;
    }

    @Override
    public boolean containsRelationType(String name) {
        return relationTypes.containsKey(name) || schemaProvider.getRelationType(name)!=null;
    }

    @Override
    public FaunusRelationType getRelationType(String name) {
        FaunusRelationType rt = relationTypes.get(name);
        if (rt==null) {
            RelationTypeDefinition def = schemaProvider.getRelationType(name);
            if (def==null) return null;
            if (def instanceof PropertyKeyDefinition) rt = new FaunusPropertyKey((PropertyKeyDefinition)def,false);
            else rt = new FaunusEdgeLabel((EdgeLabelDefinition)def,false);
            relationTypes.putIfAbsent(name,rt);
            rt = relationTypes.get(name);
        }
        assert rt!=null;
        return rt;
    }

    @Override
    public boolean containsPropertyKey(String name) {
        FaunusRelationType rt = getRelationType(name);
        return rt!=null && rt.isPropertyKey();
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        FaunusRelationType rt = getRelationType(name);
        return rt!=null && rt.isEdgeLabel();
    }

    @Override
    public FaunusPropertyKey getOrCreatePropertyKey(String name) {
        FaunusRelationType rt = getRelationType(name);
        if (rt==null) {
            relationTypes.putIfAbsent(name,new FaunusPropertyKey(schemaProvider.getPropertyKey(name),false));
            rt = relationTypes.get(name);
        }
        assert rt!=null;
        if (!(rt instanceof FaunusPropertyKey)) throw new IllegalArgumentException("Not a property key: " + name);
        return (FaunusPropertyKey)rt;
    }

    @Override
    public FaunusPropertyKey getPropertyKey(String name) {
        FaunusRelationType rt = getRelationType(name);
        Preconditions.checkArgument(rt==null || rt.isPropertyKey(),"Name does not identify a property key: ",name);
        return (FaunusPropertyKey)rt;
    }

    @Override
    public FaunusEdgeLabel getOrCreateEdgeLabel(String name) {
        FaunusRelationType rt = getRelationType(name);
        if (rt==null) {
            relationTypes.putIfAbsent(name,new FaunusEdgeLabel(schemaProvider.getEdgeLabel(name),false));
            rt = relationTypes.get(name);
        }
        assert rt!=null;
        if (!(rt instanceof FaunusEdgeLabel)) throw new IllegalArgumentException("Not an edge label: " + name);
        return (FaunusEdgeLabel)rt;
    }

    @Override
    public FaunusEdgeLabel getEdgeLabel(String name) {
        FaunusRelationType rt = getRelationType(name);
        Preconditions.checkArgument(rt==null || rt.isEdgeLabel(),"Name does not identify an edge label: ",name);
        return (FaunusEdgeLabel)rt;
    }

    public static FaunusSchemaManager getTypeManager(Configuration config) {
        return DEFAULT_MANAGER;
    }


}
