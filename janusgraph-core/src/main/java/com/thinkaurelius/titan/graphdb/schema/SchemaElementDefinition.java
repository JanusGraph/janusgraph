package com.thinkaurelius.titan.graphdb.schema;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SchemaElementDefinition {

    private final String name;
    private final long id;


    public SchemaElementDefinition(String name, long id) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public long getLongId() {
        return id;
    }


    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        return name.equals(((SchemaElementDefinition)oth).name);
    }

    @Override
    public String toString() {
        return name;
    }

}
