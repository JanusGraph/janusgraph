package com.thinkaurelius.titan.hadoop.config;


import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ReadConfiguration;

public class HadoopReadConfiguration implements ReadConfiguration {

    private final Configuration config;

    private static final Logger log =
            LoggerFactory.getLogger(HadoopReadConfiguration.class);

    public HadoopReadConfiguration(Configuration config) {
        this.config = config;
        Preconditions.checkNotNull(this.config);
    }

    @Override
    public <O> O get(String key, Class<O> datatype) {

        if (null == config.get(key))
            return null;

        if (datatype.isArray()) {
            Preconditions.checkArgument(datatype.getComponentType()==String.class,"Only string arrays are supported: %s",datatype);
            return (O)config.getStrings(key);
        } else if (Number.class.isAssignableFrom(datatype)) {
            String s = config.get(key);
            return constructFromStringArgument(datatype, s);
        } else if (datatype==String.class) {
            return (O)config.get(key);
        } else if (datatype==Boolean.class) {
            return (O)Boolean.valueOf(config.get(key));
        } else if (datatype==Class.class) {
            return (O)config.getClassByNameOrNull(key);
        } else if (datatype.isEnum()) {
            O[] constants = datatype.getEnumConstants();
            Preconditions.checkState(null != constants && 0 < constants.length, "Zero-length or undefined enum");
            String estr = config.get(key);
            for (O c : constants)
                if (c.toString().equals(estr))
                    return c;
            throw new IllegalArgumentException("No match for string \"" + estr + "\" in enum " + datatype);
//        } else if (datatype==Object.class) {
//            throw new IllegalArgumentException();
//        } else if (Duration.class.isAssignableFrom(datatype)) {
//            // This is a conceptual leak; the config layer should ideally only handle standard library types
//            Object o = config.getProperty(key);
//            if (Duration.class.isInstance(o)) {
//                return (O) o;
//            } else {
//                String[] comps = o.toString().split("\\s");
//                TimeUnit unit = null;
//                if (comps.length == 1) {
//                    //By default, times are in milli seconds
//                    unit = TimeUnit.MILLISECONDS;
//                } else if (comps.length == 2) {
//                    unit = Durations.parse(comps[1]);
//                } else {
//                    throw new IllegalArgumentException("Cannot parse time duration from: " + o.toString());
//                }
//                return (O) new StandardDuration(Long.valueOf(comps[0]), unit);
//            }
//        } else if (List.class.isAssignableFrom(datatype)) {
//            return (O) config.getProperty(key);
        } else throw new IllegalArgumentException("Unsupported data type: " + datatype);
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        Iterator<Entry<String, String>> it = config.iterator();

        List<String> keyList = new ArrayList(config.size());

        while (it.hasNext()) {
            Entry<String, String> ent = it.next();
            keyList.add(ent.getKey());
        }

        return keyList;
    }

    @Override
    public void close() {
    }

    private <O> O constructFromStringArgument(Class<O> datatype, String arg) {
        try {
            Constructor<O> ctor = datatype.getConstructor(String.class);
            return ctor.newInstance(arg);
        // ReflectiveOperationException is narrower and more appropriate than Exception, but only @since 1.7
        //} catch (ReflectiveOperationException e) {
        } catch (Exception e) {
            log.error("Failed to parse configuration string \"{}\" into type {} due to the following reflection exception", arg, datatype, e);
            throw new RuntimeException(e);
        }
    }


}
