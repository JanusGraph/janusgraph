package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.TransactionHandleConfig;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class StandardTransactionHandleConfig implements TransactionHandleConfig {

    private final Timepoint userTimestamp;
    private Timepoint commitTime;
    private final String groupName;
    private final Configuration customOptions;

    private StandardTransactionHandleConfig(String groupName,
                                            Timepoint userTimestamp,
                                            Configuration customOptions) {
        this.groupName = groupName;
        this.userTimestamp = userTimestamp;
        this.customOptions = customOptions;
    }

    @Override
    public synchronized Timepoint getTimestamp() {
        if (userTimestamp!=null) return userTimestamp;
        if (commitTime!=null) return commitTime;
        throw new IllegalStateException("No transaction timestamp has been set");
    }

    @Override
    public synchronized void setCommitTime(Timepoint time) {
        Preconditions.checkArgument(commitTime==null,"A commit time has already been set");
        this.commitTime=time;
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
        private Timepoint userTimestamp = null;
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

        public Builder timestamp(Timepoint time) {
            userTimestamp = time;
            return this;
        }

        public Builder customOptions(Configuration c) {
            customOptions = c;
            Preconditions.checkNotNull(customOptions, "Null custom options disallowed; use an empty Configuration object instead");
            return this;
        }

        public StandardTransactionHandleConfig build() {
            return new StandardTransactionHandleConfig(groupName, userTimestamp, customOptions);
        }
    }

    public static StandardTransactionHandleConfig of() {
        return new Builder().build();
    }

    public static StandardTransactionHandleConfig of(Configuration customOptions) {
        return new Builder().customOptions(customOptions).build();
    }

}
