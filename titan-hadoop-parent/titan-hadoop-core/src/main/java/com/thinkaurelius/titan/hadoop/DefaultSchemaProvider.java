package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.graphdb.schema.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DefaultSchemaProvider implements SchemaProvider {

    public static final DefaultSchemaProvider INSTANCE = new DefaultSchemaProvider();

    private DefaultSchemaProvider() {}

    @Override
    public EdgeLabelDefinition getEdgeLabel(String name) {
        return new EdgeLabelDefinition(name, FaunusElement.NO_ID, Multiplicity.MULTI,false);
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        return new PropertyKeyDefinition(name, FaunusElement.NO_ID, Cardinality.SINGLE,Object.class);
    }

    @Override
    public RelationTypeDefinition getRelationType(String name) {
        return null;
    }

    @Override
    public VertexLabelDefinition getVertexLabel(String name) {
        return new VertexLabelDefinition(name, FaunusElement.NO_ID,false,false);
    }

    public static SchemaProvider asBackupProvider(final SchemaProvider provider) {
        return asBackupProvider(provider,INSTANCE);
    }

    public static SchemaProvider asBackupProvider(final SchemaProvider provider, final SchemaProvider backup) {
        return new SchemaProvider() {
            @Override
            public EdgeLabelDefinition getEdgeLabel(String name) {
                EdgeLabelDefinition def = provider.getEdgeLabel(name);
                if (def!=null) return def;
                else return backup.getEdgeLabel(name);
            }

            @Override
            public PropertyKeyDefinition getPropertyKey(String name) {
                PropertyKeyDefinition def = provider.getPropertyKey(name);
                if (def!=null) return def;
                else return backup.getPropertyKey(name);
            }

            @Override
            public RelationTypeDefinition getRelationType(String name) {
                return provider.getRelationType(name);
            }

            @Override
            public VertexLabelDefinition getVertexLabel(String name) {
                VertexLabelDefinition def = provider.getVertexLabel(name);
                if (def!=null) return def;
                else return backup.getVertexLabel(name);
            }
        };
    }

}
