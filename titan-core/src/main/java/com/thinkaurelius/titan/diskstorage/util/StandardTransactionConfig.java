package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class StandardTransactionConfig implements TransactionHandleConfig {

    private Long timestamp = null;

    private final String metricsPrefix;
    private final TimestampProvider timestampProvider;
    private final Configuration customOptions;

    @Override
    public long getTimestamp() {
        if (timestamp==null) setTimestamp();
        assert timestamp!=null;
        return timestamp;
    }

    @Override
    public boolean hasMetricsPrefix() {
        return metricsPrefix!=null;
    }

    @Override
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    @Override
    public boolean hasTimestamp() {
        return null != timestamp;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return timestampProvider;
    }

    @Override
    public <V> V getCustomOption(ConfigOption<V> opt) {
        return customOptions.get(opt);
    }

    @Override
    public Configuration getCustomOptions() {
        return customOptions;
    }

    public static class Builder {
        private Long timestamp = null;
        private String metricsPrefix = GraphDatabaseConfiguration.getSystemMetricsPrefix();
        private TimestampProvider timestampProvider = Timestamps.NANO;
        private Configuration customOptions = Configuration.EMPTY;

        public Builder() { }

        public Builder(TransactionHandleConfig template, Long ts) {
            timestampProvider(template.getTimestampProvider());
            customOptions(template.getCustomOptions());
            metricsPrefix(template.getMetricsPrefix());

            /*
             * Copying template.getTimestamp() would be an error because
             * non-null values are ambiguous. Is it the product of lazy
             * initialization, or was it explicitly specified before
             * construction? Impossible to tell without introducing a new field
             * and interface method.
             */
            timestamp(ts);
        }

        public Builder metricsPrefix(String s) {
            metricsPrefix = s;
            return this;
        }

        public Builder timestampProvider(TimestampProvider p) {
            timestampProvider = p;
            return this;
        }

        public Builder timestamp(Long ts) {
            timestamp = ts;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardTransactionConfig build() {
            // Must have either a timestamp or a provider to lazily initialize timestamp
            // Supplying both simultaneously is allowed but unnecessary
            Preconditions.checkArgument(null != timestamp || null != timestampProvider);

            return new StandardTransactionConfig(metricsPrefix,
                    timestampProvider, timestamp, customOptions);
        }
    }

    public static StandardTransactionConfig of() {
        return new Builder().build();
    }

    public static StandardTransactionConfig of(Configuration customOptions) {
        return new Builder().customOptions(customOptions).build();
    }

    private StandardTransactionConfig(String metricsPrefix,
            TimestampProvider timestampProvider, Long timestamp,
            Configuration customOptions) {
        this.metricsPrefix = metricsPrefix;
        this.timestampProvider = timestampProvider;
        this.timestamp = timestamp;
        this.customOptions = customOptions;
    }

    private StandardTransactionConfig setTimestamp() {
        Preconditions.checkState(timestamp==null,"Timestamp has already been set");
        Preconditions.checkState(null != timestampProvider,
                "Timestamp provider must be set for lazy timestamp initialization");
        this.timestamp = timestampProvider.getTime();
        return this;
    }
}
