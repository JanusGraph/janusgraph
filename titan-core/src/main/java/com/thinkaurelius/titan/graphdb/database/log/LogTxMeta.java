package com.thinkaurelius.titan.graphdb.database.log;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum LogTxMeta {

    GROUPNAME {
        @Override
        public Object getValue(TransactionConfiguration txConfig) {
            if (!txConfig.hasGroupName()) return null;
            else return txConfig.getGroupName();
        }

        @Override
        public void setValue(TransactionBuilder builder, Object value) {
            Preconditions.checkArgument(value!=null && (value instanceof String));
            builder.setGroupName((String)value);
        }

        @Override
        public Class dataType() {
            return String.class;
        }

    },

    LOG_ID {
        @Override
        public Object getValue(TransactionConfiguration txConfig) {
            return txConfig.getLogIdentifier();
        }

        @Override
        public void setValue(TransactionBuilder builder, Object value) {
            Preconditions.checkArgument(value!=null && (value instanceof String));
            builder.setLogIdentifier((String) value);
        }
        @Override
        public Class dataType() {
            return String.class;
        }
    };

    public abstract Object getValue(TransactionConfiguration txConfig);

    public abstract void setValue(TransactionBuilder builder, Object value);

    public abstract Class dataType();

}
