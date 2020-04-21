// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import org.janusgraph.core.util.ReflectiveConfigOptionLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigNamespace extends ConfigElement {

    private final boolean isUmbrella;
    private final Map<String,ConfigElement> children = new HashMap<>();

    public ConfigNamespace(ConfigNamespace parent, String name, String description, boolean isUmbrella) {
        super(parent,name,description);
        this.isUmbrella=isUmbrella;
    }

    public ConfigNamespace(ConfigNamespace parent, String name, String description) {
        this(parent,name,description,false);
    }

    /**
     * Whether this namespace is an umbrella namespace, that is, is expects immediate sub-namespaces which are user defined.
     * @return
     */
    public boolean isUmbrella() {
        return isUmbrella;
    }

    /**
     * Whether this namespace or any parent namespace is an umbrella namespace.
     * @return
     */
    public boolean hasUmbrella() {
        return isUmbrella() || (!isRoot() && getNamespace().hasUmbrella());
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

        ConfigElement child = children.get(name);

        if (null != child) {
            return child;
        }

        // Attempt to load
        ReflectiveConfigOptionLoader.INSTANCE.loadStandard(this.getClass());
        child = children.get(name);
        if (null != child) {
            return child;
        }

        ReflectiveConfigOptionLoader.INSTANCE.loadAll(this.getClass());
        child = children.get(name);

        return child;
    }

}
