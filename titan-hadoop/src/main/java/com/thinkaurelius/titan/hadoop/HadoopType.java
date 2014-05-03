package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HadoopType {

    private enum TypeVisibility {NORMAL, HIDDEN, IMPLICIT}

    public static final HadoopType COUNT = new HadoopType(Tokens._COUNT, TypeVisibility.IMPLICIT);
    public static final HadoopType LINK = new HadoopType(Tokens._LINK, TypeVisibility.NORMAL);


    private static final Set<HadoopType> PREDEFINED_TYPES = ImmutableSet.of(COUNT, LINK);

    private final String name;
    private final TypeVisibility visibility;

    public HadoopType(String name) {
        this(checkName(name), TypeVisibility.NORMAL);
    }

    private HadoopType(String name, TypeVisibility visibility) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Invalid type name: %s", name);
        Preconditions.checkNotNull(visibility);

        this.name = name;
        this.visibility = visibility;
    }

    private static String checkName(String name) {
        //Preconditions.checkArgument(!name.startsWith("_"),"Cannot use reserved name as type: " + name);
        return name;
    }

    private static String getHiddenName(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        return "_" + name;
    }

    public String getName() {
        return name;
    }

    public boolean isHidden() {
        return visibility == TypeVisibility.HIDDEN || visibility == TypeVisibility.IMPLICIT;
    }

    public boolean isImplicit() {
        return visibility == TypeVisibility.IMPLICIT;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        else if (other == null || !getClass().isInstance(other)) return false;
        return name.equals(((HadoopType) other).name);
    }

    public static final HadoopType.Manager DEFAULT_MANAGER = new HadoopType.Manager();


    public static class Manager extends HashMap<String, HadoopType> {

        Manager() {
            for (HadoopType type : PREDEFINED_TYPES) super.put(type.getName(), type);
        }

        public final HadoopType get(final String name) {
            HadoopType type = super.get(name);
            if (type == null) {
                type = new HadoopType(name);
                super.put(name, type);
            }
            return type;
        }

    }

    public static class VertexSchema extends HashMap<HadoopType, Integer> {

        public boolean add(HadoopType type) {
            if (super.containsKey(type)) return false;
            super.put(type, super.size() + 1);
            return true;
        }

        public int getTypeID(HadoopType type) {
            Integer id = super.get(type);
            Preconditions.checkArgument(id != null, "Type is not part of this schema: " + type);
            return id;
        }


    }

}
