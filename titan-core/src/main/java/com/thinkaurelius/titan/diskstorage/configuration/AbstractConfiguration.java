package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

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
        verifyElement(option);
        return ConfigElement.getPath(option,umbrellaElements);
    }

    protected Set<String> getContainedNamespaces(ReadConfiguration config, ConfigNamespace umbrella, String... umbrellaElements) {
        verifyElement(umbrella);
        Preconditions.checkArgument(umbrella.isUmbrella());

        String prefix = ConfigElement.getPath(umbrella,umbrellaElements);
        Set<String> result = Sets.newHashSet();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix));
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
        Map<String,Object> result = Maps.newHashMap();

        for (String key : config.getKeys(prefix)) {
            Preconditions.checkArgument(key.startsWith(prefix));
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
        Preconditions.checkArgument(fixedUmbrella!=null && fixedUmbrella.length>0);
        return new Configuration() {

            private String[] concat(String... others) {
                if (others==null || others.length==0) return fixedUmbrella;
                String[] join = new String[fixedUmbrella.length+others.length];
                System.arraycopy(fixedUmbrella,0,join,0,fixedUmbrella.length);
                System.arraycopy(others,0,join,fixedUmbrella.length,others.length);
                return join;
            }

            @Override
            public boolean has(ConfigOption option, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella())
                    return config.has(option,concat(umbrellaElements));
                else
                    return config.has(option);
            }

            @Override
            public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
                if (option.getNamespace().hasUmbrella())
                    return config.get(option,concat(umbrellaElements));
                else
                    return config.get(option);
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
