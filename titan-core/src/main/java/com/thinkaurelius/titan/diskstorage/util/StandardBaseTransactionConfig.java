package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class StandardBaseTransactionConfig implements BaseTransactionConfig {

    private volatile Instant commitTime;
    private final TimestampProvider times;
    private final String groupName;
    private final Configuration customOptions;

    private StandardBaseTransactionConfig(String groupName,
                                          TimestampProvider times,
                                          Instant commitTime,
                                          Configuration customOptions) {
        Preconditions.checkArgument(customOptions!=null);
        Preconditions.checkArgument(null != times || null != commitTime);
        this.groupName = groupName;
        this.times = times;
        this.commitTime = commitTime;
        this.customOptions = customOptions;
    }

    @Override
    public synchronized Instant getCommitTime() {
        if (commitTime==null) {
            //set commit time to current time
            commitTime = times.getTime();
        }
        return commitTime;
    }

    @Override
    public synchronized void setCommitTime(Instant time) {
        Preconditions.checkArgument(commitTime==null,"A commit time has already been set");
        this.commitTime=time;
    }

    @Override
    public boolean hasCommitTime() {
        return commitTime!=null;
    }

    @Override
    public TimestampProvider getTimestampProvider() {
        return times;
    }

    @Override
    public boolean hasGroupName() {
        return groupName !=null;
    }

    @Override
    public String getGroupName() {
        return groupName;
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

        private Instant commitTime = null;
        private TimestampProvider times;
        private String groupName = GraphDatabaseConfiguration.getSystemMetricsPrefix();
        private Configuration customOptions = Configuration.EMPTY;

        public Builder() { }

        /**
         * Copies everything from {@code template} to this builder except for
         * the {@code commitTime}.
         *
         * @param template
         *            an existing transaction from which this builder will take
         *            its values
         */
        public Builder(BaseTransactionConfig template) {
            customOptions(template.getCustomOptions());
            groupName(template.getGroupName());
            timestampProvider(template.getTimestampProvider());
        }

        public Builder groupName(String group) {
            groupName = group;
            return this;
        }

        public Builder commitTime(Instant commit) {
            commitTime = commit;
            return this;
        }

        public Builder timestampProvider(TimestampProvider times) {
            this.times = times;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardBaseTransactionConfig build() {
            return new StandardBaseTransactionConfig(groupName, times, commitTime, customOptions);
        }
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times) {
        return new Builder().timestampProvider(times).build();
    }

    public static StandardBaseTransactionConfig of(TimestampProvider times, Configuration customOptions) {
        return new Builder().timestampProvider(times).customOptions(customOptions).build();
    }

}
