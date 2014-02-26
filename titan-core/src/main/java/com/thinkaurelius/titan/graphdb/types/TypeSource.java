package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeSource {

    public long getID();

    public String getName();

    public TypeDefinitionMap getDefinition();

    public Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir);

    public static class Entry {

        private final TypeSource schemaType;
        private final Object modifier;

        public Entry(TypeSource schemaType, Object modifier) {
            Preconditions.checkNotNull(schemaType);
            this.schemaType = schemaType;
            this.modifier = modifier;
        }

        public TypeSource getSchemaType() {
            return schemaType;
        }

        public Object getModifier() {
            return modifier;
        }
    }

}
