package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class UserModifiableConfiguration {

    private final ModifiableConfiguration config;

    public UserModifiableConfiguration(ModifiableConfiguration config) {
        this.config = config;
    }

    public ReadConfiguration getConfiguration() {
        return config.getBackingConfiguration();
    }

    public String get(String path) {
        ConfigElement.PathParse pp = ConfigElement.parse(config.getRootNamespace(),path);
        if (pp.element.isNamespace()) {
            ConfigNamespace ns = (ConfigNamespace)pp.element;
            StringBuilder s = new StringBuilder();
            if (ns.isUmbrella() && !pp.lastIsUmbrella) {
                for (String sub : config.getContainedNamespaces(ns,pp.umbrellaElements)) {
                    s.append("+ ").append(sub).append("\n");
                }
            } else {
                for (ConfigElement element : ns.getChildren()) {
                    s.append(toString(element)).append("\n");
                }
            }
            return s.toString();
        } else {
            Object value;
            try {
                value = config.get((ConfigOption)pp.element,pp.umbrellaElements);
            } catch (IllegalStateException e) {
                return "value missing";
            }
            if (value==null) return "null";
            if (value.getClass().isArray()) {
                StringBuilder s = new StringBuilder();
                s.append("[");
                for (int i=0;i<Array.getLength(value);i++) {
                    if (i>0) s.append(",");
                    s.append(Array.get(value,i));
                }
                s.append("]");
                return s.toString();
            } else return String.valueOf(value);
        }
    }

    private static String toString(ConfigElement element) {
        String result = element.getName();
        if (element.isNamespace()) {
            return "+ " + result;
        } else {
            result = "- " + result;
            ConfigOption option = (ConfigOption)element;
            result+= " [";
            switch (option.getType()) {
                case FIXED: result+="f"; break;
                case GLOBAL_OFFLINE: result+="g!"; break;
                case GLOBAL: result+="g"; break;
                case MASKABLE: result+="m"; break;
                case LOCAL: result+="l"; break;
            }
            result+=","+option.getDatatype().getSimpleName();
            result+=","+option.getDefaultValue();
            result+="]";
        }
        String desc = element.getDescription();
        result+="\t"+desc.substring(0, Math.min(desc.length(), 30));
        return result;
    }

    public void set(String path, Object value, String... paras) {
        ConfigElement.PathParse pp = ConfigElement.parse(config.getRootNamespace(),path);
        Preconditions.checkArgument(pp.element.isOption(),"Need to provide configuration option - not namespace: %s",path);
        ConfigOption option = (ConfigOption)pp.element;
        if (option.getType()== ConfigOption.Type.GLOBAL_OFFLINE && config.isFrozenConfiguration()) {
            Preconditions.checkArgument(paras != null && paras.length > 0 && paras[0].trim().equalsIgnoreCase("force"), "Need to force global parameter update for: %s", option);
        }
        if (option.getDatatype().isArray()) {
            Class arrayType = option.getDatatype().getComponentType();
            Object arr;
            if (value.getClass().isArray()) {
                int size = Array.getLength(value);
                arr = Array.newInstance(arrayType,size);
                for (int i=0;i<size;i++) {
                    Array.set(arr,i,convertBasic(Array.get(value,i),arrayType));
                }
            } else {
                arr = Array.newInstance(arrayType,1);
                Array.set(arr,0,convertBasic(value,arrayType));
            }
            value = arr;
        } else {
            value = convertBasic(value,option.getDatatype());
        }
        config.set(option,value,pp.umbrellaElements);
    }

    public void close() {
        config.close();
    }

    private static final Object convertBasic(Object value, Class datatype) {
        if (Number.class.isAssignableFrom(datatype)) {
            Preconditions.checkArgument(value instanceof Number,"Expected a number but got: %s",value);
            Number n = (Number)value;
            if (datatype==Long.class) {
                return n.longValue();
            } else if (datatype==Integer.class) {
                return n.intValue();
            } else if (datatype==Short.class) {
                return n.shortValue();
            } else if (datatype==Byte.class) {
                return n.byteValue();
            } else if (datatype==Float.class) {
                return n.floatValue();
            } else if (datatype==Double.class) {
                return n.doubleValue();
            } else throw new IllegalArgumentException("Unexpected number data type: " + datatype);
        } else if (datatype==Boolean.class) {
            Preconditions.checkArgument(value instanceof Boolean,"Expected boolean value: %s",value);
            return value;
        } else if (datatype==String.class) {
            Preconditions.checkArgument(value instanceof String,"Expected string value: %s",value);
            return value;
        } else throw new IllegalArgumentException("Unexpected data type: " + datatype.getClasses() );
    }

}
