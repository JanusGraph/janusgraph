package com.thinkaurelius.titan.diskstorage.configuration.backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.configuration.ReadConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;

/**
 * {@link ReadConfiguration} wrapper for Apache Configuration
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CommonsConfiguration implements WriteConfiguration {

    private final Configuration config;

    public CommonsConfiguration(Configuration config) {
        Preconditions.checkArgument(config!=null);
        this.config = config;
    }

    public Configuration getCommonConfiguration() {
        return config;
    }

    @Override
    public<O> O get(String key, Class<O> datatype) {
        if (!config.containsKey(key)) return null;

        if (datatype.isArray()) {
            Preconditions.checkArgument(datatype.getComponentType()==String.class,"Only string arrays are supported: %s",datatype);
            return (O)config.getStringArray(key);
        } else if (Number.class.isAssignableFrom(datatype)) {
            return (O)config.getProperty(key);
        } else if (datatype==String.class) {
            return (O)config.getString(key);
        } else if (datatype==Boolean.class) {
            return (O)new Boolean(config.getBoolean(key));
        } else if (datatype==Object.class) {
            return (O)config.getProperty(key);
        } else throw new IllegalArgumentException("Unsupported data type: " + datatype);
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        List<String> result = Lists.newArrayList();
        Iterator<String> keys;
        if (StringUtils.isNotBlank(prefix)) keys = config.getKeys(prefix);
        else keys = config.getKeys();
        while (keys.hasNext()) result.add(keys.next());
        return result;
    }

    @Override
    public void close() {
        //Do nothing
    }

    @Override
    public <O> void set(String key, O value) {
        config.setProperty(key,value);
    }

    @Override
    public void remove(String key) {
        config.clearProperty(key);
    }

    @Override
    public WriteConfiguration clone() {
        BaseConfiguration copy = new BaseConfiguration();
        copy.copy(config);
        return new CommonsConfiguration(copy);
    }

}
