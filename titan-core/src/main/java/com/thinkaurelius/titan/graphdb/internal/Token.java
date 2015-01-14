package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Token {

    public static final char SEPARATOR_CHAR = '%';

    public static final char SYSTEM_PREFIX = '^';

    public static final String getSeparatedName(String... components) {
        for (String component : components) verifyName(component);
        return StringUtils.join(components,SEPARATOR_CHAR);
    }

    public static final void verifyName(String name) {
        Preconditions.checkArgument(name.indexOf(Token.SEPARATOR_CHAR) < 0,
                "Name can not contains reserved character %s: %s", Token.SEPARATOR_CHAR, name);
    }

    public static final String[] splitSeparatedName(String name) {
        return name.split(SEPARATOR_CHAR+"");
    }

    public static final String INTERNAL_INDEX_NAME = "internalindex";

    public static boolean isSystemName(String name) {
        return name!=null && name.length()>0 && name.charAt(0)==SYSTEM_PREFIX;
    }

    public static String makeSystemName(String name) {
        return SYSTEM_PREFIX + name;
    }



}
