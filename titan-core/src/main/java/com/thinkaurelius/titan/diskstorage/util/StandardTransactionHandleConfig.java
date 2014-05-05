package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.time.TimestampProvider;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class StandardTransactionHandleConfig implements TransactionHandleConfig {

    private final Timepoint startTime;
    private Timepoint commitTime;
    private final String groupName;
    private final Configuration customOptions;

    private StandardTransactionHandleConfig(String groupName, Timepoint startTime,
                                            Timepoint commitTime,
                                            Configuration customOptions) {
        Preconditions.checkArgument(startTime!=null && customOptions!=null);
        this.groupName = groupName;
        this.startTime = startTime;
        this.commitTime = commitTime;
        this.customOptions = customOptions;
    }

    @Override
    public synchronized Timepoint getCommitTime() {
        if (commitTime==null) {
            //set commit time to current time
            commitTime = startTime.getProvider().getTime();
        }
        return commitTime;
    }

    @Override
    public synchronized void setCommitTime(Timepoint time) {
        Preconditions.checkArgument(commitTime==null,"A commit time has already been set");
        this.commitTime=time;
    }

    @Override
    public boolean hasCommitTime() {
        return commitTime!=null;
    }

    @Override
    public Timepoint getStartTime() {
        return startTime;
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

        private Timepoint startTime = null;
        private Timepoint commitTime = null;
        private String groupName = GraphDatabaseConfiguration.getSystemMetricsPrefix();
        private Configuration customOptions = Configuration.EMPTY;

        public Builder() { }

        public Builder(TransactionHandleConfig template) {
            customOptions(template.getCustomOptions());
            groupName(template.getGroupName());
        }

        public Builder groupName(String group) {
            groupName = group;
            return this;
        }

        public Builder startTime(Timepoint start) {
            startTime = start;
            return this;
        }

        public Builder commitTime(Timepoint commit) {
            commitTime = commit;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardTransactionHandleConfig build() {
            return new StandardTransactionHandleConfig(groupName, startTime, commitTime, customOptions);
        }
    }

    public static StandardTransactionHandleConfig of(TimestampProvider times) {
        return new Builder().startTime(times.getTime()).build();
    }

    public static StandardTransactionHandleConfig of(TimestampProvider times, Configuration customOptions) {
        return new Builder().startTime(times.getTime()).customOptions(customOptions).build();
    }

}
