package com.thinkaurelius.titan.diskstorage.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseCompatLoader {

    private static final Logger log = LoggerFactory.getLogger(HBaseCompatLoader.class);

    private static final String DEFAULT_HBASE_COMPAT_VERSION = "1.0";

    private static final String DEFAULT_HBASE_CLASS_NAME = "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat1_00";

    private static HBaseCompat cachedCompat;

    public synchronized static HBaseCompat getCompat(String classOverride) {

        if (null != cachedCompat) {
            log.debug("Returning cached HBase compatibility layer: {}", cachedCompat);
            return cachedCompat;
        }

        HBaseCompat compat;
        String className = null;
        String classNameSource = null;

        if (null != classOverride) {
            className = classOverride;
            classNameSource = "from explicit configuration";
        } else {
            String hbaseVersion = VersionInfo.getVersion();
            for (String supportedVersion : Arrays.asList("0.94", "0.96", "0.98", "1.0")) {
                if (hbaseVersion.startsWith(supportedVersion + ".")) {
                    className = "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat" + supportedVersion.replaceAll("\\.", "_");
                    classNameSource = "supporting runtime HBase version " + hbaseVersion;
                    break;
                }
            }
            if (null == className) {
                log.info("The HBase version {} is not explicitly supported by Titan.  " +
                         "Loading Titan's compatibility layer for its most recent supported HBase version ({})",
                        hbaseVersion, DEFAULT_HBASE_COMPAT_VERSION);
                className = DEFAULT_HBASE_CLASS_NAME;
                classNameSource = " by default";
            }
        }

        final String errTemplate = " when instantiating HBase compatibility class " + className;

        try {
            compat = (HBaseCompat)Class.forName(className).newInstance();
            log.info("Instantiated HBase compatibility layer {}: {}", classNameSource, compat.getClass().getCanonicalName());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        }

        return cachedCompat = compat;
    }
}
