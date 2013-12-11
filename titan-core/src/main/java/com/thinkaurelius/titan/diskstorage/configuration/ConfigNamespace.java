package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigNamespace extends ConfigElement {

    private final boolean isUmbrella;
    private final Map<String,ConfigElement> children = Maps.newHashMap();

    public ConfigNamespace(ConfigNamespace parent, String name, String description, boolean isUmbrella) {
        super(parent,name,description);
        this.isUmbrella=isUmbrella;
    }

    public ConfigNamespace(ConfigNamespace parent, String name, String description) {
        this(parent,name,description,false);
    }

    public boolean isUmbrella() {
        return isUmbrella;
    }

    @Override
    public boolean isOption() {
        return false;
    }

    void registerChild(ConfigElement element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(element.getNamespace()==this,"Configuration element registered with wrong namespace");
        Preconditions.checkArgument(!children.containsKey(element.getName()),
                "A configuration element with the same name has already been added to this namespace: %s",element.getName());
        children.put(element.getName(),element);
    }

    public Iterable<ConfigElement> getChildren() {
        return children.values();
    }

    public ConfigElement getChild(String name) {
        return children.get(name);
    }

}
