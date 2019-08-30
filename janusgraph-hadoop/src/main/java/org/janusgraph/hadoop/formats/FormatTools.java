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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 */
public class FormatTools {

    public static Class getBaseOutputFormatClass(final Job job) {
        try {
            if (LazyOutputFormat.class.isAssignableFrom(job.getOutputFormatClass())) {
                Class<OutputFormat> baseClass = (Class<OutputFormat>)
                    job.getConfiguration().getClass(LazyOutputFormat.OUTPUT_FORMAT, null);
                return (null == baseClass) ? job.getOutputFormatClass() : baseClass;
            }
            return job.getOutputFormatClass();
        } catch (Exception e) {
            return null;
        }
    }
}
