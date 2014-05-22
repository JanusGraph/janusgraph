package com.thinkaurelius.titan.diskstorage.hbase;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseCompatLoader {

    private static final Logger log = LoggerFactory
            .getLogger(HBaseCompatLoader.class);

    /**
     * If this key is present in either the JVM system properties or the process
     * environment (checked in the listed order, first hit wins), then its value
     * must be the full package and class name of an implementation of
     * {@link HBaseCompat} that has a no-arg public constructor.
     * <p>
     * When this <b>is not</b> set, Titan attempts to automatically detect the
     * HBase runtime version by calling {@link VersionInfo#getVersion()}. Titan
     * then checks the returned version string against a hard-coded list of
     * supported version prefixes and instantiates the associated compat layer
     * if a match is found.
     * <p>
     * When this <b>is</b> set, Titan will not call
     * {@code VersionInfo.getVersion()} or read its hard-coded list of supported
     * version prefixes. Titan will instead attempt to instantiate the class
     * specified (via the no-arg constructor which must exist) and then attempt
     * to cast it to HBaseCompat and use it as such. Titan will assume the
     * supplied implementation is compatible with the runtime HBase version and
     * make no attempt to verify that assumption.
     * <p>
     * Setting this key incorrectly could cause runtime exceptions at best or
     * silent data corruption at worst. This setting is intended for users
     * running exotic HBase implementations that don't support VersionInfo or
     * implementations which return values from {@code VersionInfo.getVersion()}
     * that are inconsistent with Apache's versioning convention. It may also be
     * useful to users who want to run against a new release of HBase that Titan
     * doesn't yet officially support.
     *
     */
    public static final String TITAN_HBASE_COMPAT_CLASS_KEY = "TITAN_HBASE_COMPAT_CLASS";
    private static final String TITAN_HBASE_COMPAT_CLASS;

    static {

        String s;

        if (null != (s = System.getProperty(TITAN_HBASE_COMPAT_CLASS_KEY))) {
            log.info("Read {} from system properties: {}", TITAN_HBASE_COMPAT_CLASS_KEY, s);
        } else if (null != (s = System.getenv(TITAN_HBASE_COMPAT_CLASS_KEY))) {
            log.info("Read {} from process environment: {}", TITAN_HBASE_COMPAT_CLASS_KEY, s);
        } else {
            log.debug("Could not read {} from system properties or process environment; using HBase VersionInfo to resolve compat layer", TITAN_HBASE_COMPAT_CLASS_KEY);
        }

        TITAN_HBASE_COMPAT_CLASS = s;
    }

    private static HBaseCompat cachedCompat;

    public synchronized static HBaseCompat getCompat() {

        if (null != cachedCompat) {
            log.debug("Returning cached HBase compatibility layer: {}", cachedCompat);
            return cachedCompat;
        }

        HBaseCompat compat = null;
        String className = null;
        String classNameSource = null;

        if (null != TITAN_HBASE_COMPAT_CLASS) {
            className = TITAN_HBASE_COMPAT_CLASS;
            classNameSource = "from " + TITAN_HBASE_COMPAT_CLASS_KEY + " setting";
        } else {
            String hbaseVersion = VersionInfo.getVersion();
            for (String supportedVersion : Arrays.asList("0.94", "0.96", "0.98")) {
                if (hbaseVersion.startsWith(supportedVersion + ".")) {
                    className = "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat" + supportedVersion.replaceAll("\\.", "_");
                    classNameSource = "supporting runtime HBase version " + hbaseVersion;
                    break;
                }
            }
            if (null == className) {
                throw new RuntimeException("Unrecognized or unsupported HBase version " + hbaseVersion);
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

        if (null == compat) {
            throw new RuntimeException("Unable to locate or instantiate HBase compat layer");
        }

        return cachedCompat = compat;
    }
}
