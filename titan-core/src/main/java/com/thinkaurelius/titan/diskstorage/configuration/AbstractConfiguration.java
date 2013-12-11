package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

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

    public abstract void close();

}
