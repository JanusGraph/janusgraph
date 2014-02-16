#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
HTrace = org.cloudera.htrace.Trace
java_import org.cloudera.htrace.Sampler
java_import org.apache.hadoop.hbase.trace.SpanReceiverHost

module Shell
  module Commands
    class Trace < Command
      def help
        return <<-EOF
Start or Stop tracing using HTrace.
Always returns true if tracing is running, otherwise false.
If the first argument is 'start', new span is started.
If the first argument is 'stop', current running span is stopped.
('stop' returns false on success.)
If the first argument is 'status', just returns if or not tracing is running.
On 'start'-ing, you can optionally pass the name of span as the second argument.
The default name of span is 'HBaseShell'.
Repeating 'start' does not start nested span.

Examples:

  hbase> trace 'start'
  hbase> trace 'status'
  hbase> trace 'stop'

  hbase> trace 'start', 'MySpanName'
  hbase> trace 'stop'

EOF
      end

      def command(startstop="status", spanname="HBaseShell")
        format_and_return_simple_command do 
          trace(startstop, spanname)
        end
      end

      def trace(startstop, spanname)
        @@receiver ||= SpanReceiverHost.getInstance(@shell.hbase.configuration)
        if startstop == "start"
          if not tracing?
            @@tracescope = HTrace.startSpan(spanname, Sampler.ALWAYS)
          end
        elsif startstop == "stop"
          if tracing?
            @@tracescope.close()
          end
        end
        tracing?
      end

      def tracing?()
        HTrace.isTracing()
      end

    end
  end
end
