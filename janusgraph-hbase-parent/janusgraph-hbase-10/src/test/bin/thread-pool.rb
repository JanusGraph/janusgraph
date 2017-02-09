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
# File passed to org.jruby.Main by bin/hbase.  Pollutes jirb with hbase imports
# and hbase  commands and then loads jirb.  Outputs a banner that tells user
# where to find help, shell version, and loads up a custom hirb.

require 'thread'

class ThreadPool
  def initialize(poolsize)
    @queue = Queue.new
    @poolsize = poolsize  
    @pool = Array.new(@poolsize) do |i|
      Thread.new do
        Thread.current[:id] = i
        catch(:close) do
          loop do
            job, args = @queue.pop
            job.call(*args)
          end
        end
      end
    end
  end

  def launch(*args, &block)
    @queue << [block, args]
  end

  def stop
    @poolsize.times do
      launch { throw :close }
    end
    @pool.map(&:join)
  end
end
