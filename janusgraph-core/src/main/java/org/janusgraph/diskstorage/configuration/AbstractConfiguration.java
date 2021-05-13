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
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractConfiguration implements Configuration {

    private final ConfigNamespace root;


    protected AbstractConfiguration(ConfigNamespace root) {
        Preconditions.checkNotNull(root);
        Preconditions.checkArgument(!root.isUmbrella(),"Root cannot be an umbrella namespace");
        this.root = root;
    }

    public ConfigNamespace getRootNamespace() {
        return root;
    }

    protected void verifyElement(ConfigElement element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(element.getRoot().equals(root),"Configuration element is not associated with this configuration: %s",element);
    }

    protected String getPath(ConfigElement option, String... umbrellaElements) {
        return getPath(option, false, umbrellaElements);
    }

    protected String getPath(ConfigElement option, boolean includeRoot, String... umbrellaElements) {
        verifyElement(option);
        return ConfigElement.getPath(option, includeRoot, umbrellaElements);
    }

    protected Set<String> getContainedNamespaces(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);
        Preconditions.checkArgument(umbrella.isUmbrella(), "config namespace must be an umbrella namespace");

        String prefix = ConfigElement.getPath(umbrella,umbrellaElements);
        Set<String> result = new HashSet<>();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix), "key [%s] does not start with prefix [%s]", key, prefix);
            String sub = key.substring(prefix.length()+1).trim();
            if (!sub.isEmpty()) {
                String ns = ConfigElement.getComponents(sub)[0];
                Preconditions.checkArgument(StringUtils.isNotBlank(ns),"Invalid sub-namespace for key: %s",key);
                result.add(ns);
            }
        }
        return result;
    }

    protected Map<String,Object> getSubset(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);

        String prefix = umbrella.isRoot() ? "" : ConfigElement.getPath(umbrella, umbrellaElements);
        Map<String,Object> result = new HashMap<>();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix), "key [%s] does not start with prefix [%s]", key, prefix);
            // A zero-length prefix is a root.  A positive-length prefix
            // is not a root and we should tack on an additional character
            // to consume the dot between the prefix and the rest of the key.
            int startIndex = umbrella.isRoot() ? prefix.length() : prefix.length() + 1;
            String sub = key.substring(startIndex).trim();
            if (!sub.isEmpty()) {
                result.put(sub,config.get(key,Object.class));
            }
        }
        return result;
    }

    protected static Configuration restrictTo(final Configuration config, final String... fixedUmbrella) {
        Preconditions.checkArgument(fixedUmbrella!=null && fixedUmbrella.length>0, "No fixedUmbrella is given");
        return new Configuration() {

            private String[] concat(String... others) {
                if (others==null || others.length==0) return fixedUmbrella;
                String[] join = new String[fixedUmbrella.length+others.length];
                System.arraycopy(fixedUmbrella,0,join,0,fixedUmbrella.length);
                System.arraycopy(others,0,join,fixedUmbrella.length,others.length);
                return join;
            }

            @Override
            public boolean has(ConfigOption option, boolean includeRoot, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella())
                    return config.has(option, includeRoot, concat(umbrellaElements));
                else
                    return config.has(option, includeRoot);
            }

            @Override
            public <O> O get(ConfigOption<O> option, boolean includeRoot, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella())
                    return config.get(option, includeRoot, concat(umbrellaElements));
                else
                    return config.get(option, includeRoot);
            }

            @Override
            public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
                return config.getContainedNamespaces(umbrella,concat(umbrellaElements));
            }

            @Override
            public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
                return config.getSubset(umbrella,concat(umbrellaElements));
            }

            @Override
            public Configuration restrictTo(String... umbrellaElements) {
                return config.restrictTo(concat(umbrellaElements));
            }
        };
    }

    public abstract void close();

}
