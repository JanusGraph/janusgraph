package com.thinkaurelius.titan.diskstorage.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseCompatLoader {

    private static HBaseCompat cachedCompat;

    private static final Logger log =
            LoggerFactory.getLogger(HBaseCompatLoader.class);

    public synchronized static HBaseCompat getCompat() {

        if (null != cachedCompat) {
            log.debug("Returning cached HBase compatibility layer: {}", cachedCompat);
            return cachedCompat;
        }

        String hbaseVersion = VersionInfo.getVersion();
        HBaseCompat compat = null;

        for (String supportedVersion : Arrays.asList("0.94", "0.96")) {
            if (hbaseVersion.startsWith(supportedVersion + ".")) {
                String className = "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat" + supportedVersion.replaceAll("\\.", "_");
                try {
                    compat = (HBaseCompat)Class.forName(className).newInstance();
                    log.info("Instantiated HBase {} compatibility layer for runtime HBase version {}: {}",
                            supportedVersion, hbaseVersion, compat);
                } catch (ReflectiveOperationException e) {
                    String errMsg = "Unable to load HBase " + hbaseVersion + " compatibility class " + className;
                    throw new RuntimeException(errMsg, e);
                }
            }
        }

        if (null == compat) {
            throw new RuntimeException("Unrecognized or unsupported HBase version " + hbaseVersion);
        }

        return cachedCompat = compat;
    }
}
