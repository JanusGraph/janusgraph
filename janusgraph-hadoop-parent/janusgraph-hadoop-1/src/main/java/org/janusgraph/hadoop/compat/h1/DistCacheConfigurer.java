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

package org.janusgraph.hadoop.compat.h1;

import org.janusgraph.hadoop.config.job.AbstractDistCacheConfigurer;
import org.janusgraph.hadoop.config.job.JobClasspathConfigurer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class DistCacheConfigurer extends AbstractDistCacheConfigurer implements JobClasspathConfigurer {

    public DistCacheConfigurer(String mapredJarFilename) {
        super(mapredJarFilename);
    }

    @Override
    public void configure(Job job) throws IOException {

        for (Path p : getLocalPaths()) {
            Configuration conf = job.getConfiguration();
            FileSystem jobFS = FileSystem.get(conf);
            FileSystem localFS = FileSystem.getLocal(conf);
            Path stagedPath = uploadFileIfNecessary(localFS, p, jobFS);
            DistributedCache.addFileToClassPath(stagedPath, conf, jobFS);
        }

        // We don't really need to set a mapred job jar here,
        // but doing so suppresses a warning
        String mj = getMapredJar();
        if (null != mj)
            job.getConfiguration().set(Hadoop1Compat.CFG_JOB_JAR, mj);
    }
}
