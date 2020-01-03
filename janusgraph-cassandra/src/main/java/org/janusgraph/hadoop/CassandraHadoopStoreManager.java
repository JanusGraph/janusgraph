// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.hadoop;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.janusgraph.hadoop.formats.cassandra.CassandraBinaryInputFormat;

public class CassandraHadoopStoreManager implements HadoopStoreManager {

    private IPartitioner partitioner;

    public CassandraHadoopStoreManager(IPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    @Override
    public Class<? extends InputFormat> getInputFormat(Configuration hadoopConf) {
        hadoopConf.set("cassandra.input.partitioner.class", partitioner.getClass().getName());
        return CassandraBinaryInputFormat.class;
    }
}
