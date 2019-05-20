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

module Shell
  module Commands
    class Alter < Command
      def help
        return <<-EOF
Alter a table. If the "hbase.online.schema.update.enable" property is set to
false, then the table must be disabled (see help 'disable'). If the 
"hbase.online.schema.update.enable" property is set to true, tables can be 
altered without disabling them first. Altering enabled tables has caused problems 
in the past, so use caution and test it before using in production. 

You can use the alter command to add, 
modify or delete column families or change table configuration options.
Column families work in a similar way as the 'create' command. The column family
specification can either be a name string, or a dictionary with the NAME attribute.
Dictionaries are described in the output of the 'help' command, with no arguments.

For example, to change or add the 'f1' column family in table 't1' from 
current value to keep a maximum of 5 cell VERSIONS, do:

  hbase> alter 't1', NAME => 'f1', VERSIONS => 5

You can operate on several column families:

  hbase> alter 't1', 'f1', {NAME => 'f2', IN_MEMORY => true}, {NAME => 'f3', VERSIONS => 5}

To delete the 'f1' column family in table 'ns1:t1', use one of:

  hbase> alter 'ns1:t1', NAME => 'f1', METHOD => 'delete'
  hbase> alter 'ns1:t1', 'delete' => 'f1'

You can also change table-scope attributes like MAX_FILESIZE, READONLY, 
MEMSTORE_FLUSHSIZE, DURABILITY, etc. These can be put at the end;
for example, to change the max size of a region to 128MB, do:

  hbase> alter 't1', MAX_FILESIZE => '134217728'

You can add a table coprocessor by setting a table coprocessor attribute:

  hbase> alter 't1',
    'coprocessor'=>'hdfs:///foo.jar|com.foo.FooRegionObserver|1001|arg1=1,arg2=2'

Since you can have multiple coprocessors configured for a table, a
sequence number will be automatically appended to the attribute name
to uniquely identify it.

The coprocessor attribute must match the pattern below in order for
the framework to understand how to load the coprocessor classes:

  [coprocessor jar file location] | class name | [priority] | [arguments]

You can also set configuration settings specific to this table or column family:

  hbase> alter 't1', CONFIGURATION => {'hbase.hregion.scan.loadColumnFamiliesOnDemand' => 'true'}
  hbase> alter 't1', {NAME => 'f2', CONFIGURATION => {'hbase.hstore.blockingStoreFiles' => '10'}}

You can also remove a table-scope attribute:

  hbase> alter 't1', METHOD => 'table_att_unset', NAME => 'MAX_FILESIZE'

  hbase> alter 't1', METHOD => 'table_att_unset', NAME => 'coprocessor$1'

You can also set REGION_REPLICATION:

  hbase> alter 't1', {REGION_REPLICATION => 2}

There could be more than one alteration in one command:

  hbase> alter 't1', { NAME => 'f1', VERSIONS => 3 }, 
   { MAX_FILESIZE => '134217728' }, { METHOD => 'delete', NAME => 'f2' },
   OWNER => 'johndoe', METADATA => { 'mykey' => 'myvalue' }
EOF
      end

      def command(table, *args)
        format_simple_command do
          admin.alter(table, true, *args)
        end
      end
    end
  end
end
