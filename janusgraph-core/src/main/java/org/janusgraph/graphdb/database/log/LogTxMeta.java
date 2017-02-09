// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.log;

import com.google.common.base.Preconditions;
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.graphdb.log.StandardTransactionId;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;

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
            builder.groupName((String) value);
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
            builder.logIdentifier((String) value);
        }
        @Override
        public Class dataType() {
            return String.class;
        }
    },

    SOURCE_TRANSACTION {
        @Override
        public Object getValue(TransactionConfiguration txConfig) {
            return null;
        }

        @Override
        public void setValue(TransactionBuilder builder, Object value) {
            //Do nothing
        }

        @Override
        public Class dataType() {
            return StandardTransactionId.class;
        }
    }


    ;

    public abstract Object getValue(TransactionConfiguration txConfig);

    public abstract void setValue(TransactionBuilder builder, Object value);

    public abstract Class dataType();

}
