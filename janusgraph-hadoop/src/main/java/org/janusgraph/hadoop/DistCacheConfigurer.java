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

package org.janusgraph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.janusgraph.hadoop.config.job.AbstractDistCacheConfigurer;
import org.janusgraph.hadoop.config.job.JobClasspathConfigurer;

import java.io.IOException;

public class DistCacheConfigurer extends AbstractDistCacheConfigurer implements JobClasspathConfigurer {

    public DistCacheConfigurer(String mapReduceJarFileName) {
        super(mapReduceJarFileName);
    }

    @Override
    public void configure(Job job) throws IOException {

        Configuration conf = job.getConfiguration();
        FileSystem localFS = FileSystem.getLocal(conf);
        FileSystem jobFS = FileSystem.get(conf);

        for (Path p : getLocalPaths()) {
            Path stagedPath = uploadFileIfNecessary(localFS, p, jobFS);
            // Calling this method decompresses the archive and makes Hadoop
            // handle its class files individually.  This leads to crippling
            // overhead times (10+ seconds) even with the LocalJobRunner
            // courtesy of o.a.h.yarn.util.FSDownload.changePermissions
            // copying and changing the mode of each classfile copy file individually.
            //job.addArchiveToClassPath(p);
            // Just add the compressed archive instead:
            job.addFileToClassPath(stagedPath);
        }

        // We don't really need to set a map reduce job jar here,
        // but doing so suppresses a warning
        String mj = getMapredJar();
        if (null != mj)
            job.setJar(mj);
    }
}
