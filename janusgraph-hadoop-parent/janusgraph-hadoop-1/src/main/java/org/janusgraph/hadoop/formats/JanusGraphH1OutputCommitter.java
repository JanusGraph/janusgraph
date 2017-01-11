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

package org.janusgraph.hadoop.formats;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class JanusGraphH1OutputCommitter extends OutputCommitter {
    private final JanusGraphH1OutputFormat tof;

    public JanusGraphH1OutputCommitter(JanusGraphH1OutputFormat tof) {
        this.tof = tof;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return tof.hasModifications(taskContext.getTaskAttemptID());
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
        tof.commit(taskContext.getTaskAttemptID());
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
        tof.abort(taskContext.getTaskAttemptID());
    }
}
