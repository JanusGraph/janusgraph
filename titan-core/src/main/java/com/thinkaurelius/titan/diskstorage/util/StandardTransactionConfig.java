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

    private final Long timestamp;
    private final String metricsPrefix;
    private final Configuration customOptions;

    @Override
    public Long getTimestamp() {
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
        private Configuration customOptions = Configuration.EMPTY;

        public Builder() { }

        public Builder(TransactionHandleConfig template, Long ts) {
            customOptions(template.getCustomOptions());
            metricsPrefix(template.getMetricsPrefix());
            timestamp(ts);
        }

        public Builder metricsPrefix(String s) {
            metricsPrefix = s;
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
            return new StandardTransactionConfig(metricsPrefix, timestamp, customOptions);
        }
    }

    public static StandardTransactionConfig of() {
        return new Builder().build();
    }

    public static StandardTransactionConfig of(Configuration customOptions) {
        return new Builder().customOptions(customOptions).build();
    }

    private StandardTransactionConfig(String metricsPrefix,
            Long timestamp,
            Configuration customOptions) {
        this.metricsPrefix = metricsPrefix;
        this.timestamp = timestamp;
        this.customOptions = customOptions;
    }
}
