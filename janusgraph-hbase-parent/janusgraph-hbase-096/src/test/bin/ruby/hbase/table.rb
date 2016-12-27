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

include Java

# Wrapper for org.apache.hadoop.hbase.client.HTable

module Hbase
  class Table
    include HBaseConstants

    @@thread_pool = nil

    # Add the command 'name' to table s.t. the shell command also called via 'name'
    # and has an internal method also called 'name'.
    #
    # e.g. name = scan, adds table.scan which calls Scan.scan
    def self.add_shell_command(name)
      self.add_command(name, name, name)
    end

    # add a named command to the table instance
    #
    # name - name of the command that should added to the table
    #    (eg. sending 'scan' here would allow you to do table.scan)
    # shell_command - name of the command in the shell
    # internal_method_name - name of the method in the shell command to forward the call
    def self.add_command(name, shell_command, internal_method_name)
      method  = name.to_sym
      self.class_eval do
        define_method method do |*args|
            @shell.internal_command(shell_command, internal_method_name, self, *args)
         end
      end
    end
    
    # General help for the table
    # class level so we can call it from anywhere
    def self.help
      return <<-EOF
Help for table-reference commands.

You can either create a table via 'create' and then manipulate the table via commands like 'put', 'get', etc.
See the standard help information for how to use each of these commands.

However, as of 0.96, you can also get a reference to a table, on which you can invoke commands.
For instance, you can get create a table and keep around a reference to it via:

   hbase> t = create 't', 'cf'

Or, if you have already created the table, you can get a reference to it:

   hbase> t = get_table 't'

You can do things like call 'put' on the table:

  hbase> t.put 'r', 'cf:q', 'v'

which puts a row 'r' with column family 'cf', qualifier 'q' and value 'v' into table t.

To read the data out, you can scan the table:

  hbase> t.scan

which will read all the rows in table 't'.

Essentially, any command that takes a table name can also be done via table reference.
Other commands include things like: get, delete, deleteall,
get_all_columns, get_counter, count, incr. These functions, along with
the standard JRuby object methods are also available via tab completion.

For more information on how to use each of these commands, you can also just type:

   hbase> t.help 'scan'

which will output more information on how to use that command.

You can also do general admin actions directly on a table; things like enable, disable,
flush and drop just by typing:

   hbase> t.enable
   hbase> t.flush
   hbase> t.disable
   hbase> t.drop

Note that after dropping a table, your reference to it becomes useless and further usage
is undefined (and not recommended).
EOF
      end
    
    #---------------------------------------------------------------------------------------------

    # let external objects read the underlying table object
    attr_reader :table
    # let external objects read the table name
    attr_reader :name

    def initialize(configuration, table_name, shell)
      if @@thread_pool then
        @table = org.apache.hadoop.hbase.client.HTable.new(configuration, table_name.to_java_bytes, @@thread_pool)
      else
        @table = org.apache.hadoop.hbase.client.HTable.new(configuration, table_name)
        @@thread_pool = @table.getPool()
      end
      @name = table_name
      @shell = shell
      @converters = Hash.new()
    end

    # Note the below methods are prefixed with '_' to hide them from the average user, as
    # they will be much less likely to tab complete to the 'dangerous' internal method
    #----------------------------------------------------------------------------------------------
    # Put a cell 'value' at specified table/row/column
    def _put_internal(row, column, value, timestamp = nil)
      p = org.apache.hadoop.hbase.client.Put.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if timestamp
        p.add(family, qualifier, timestamp, value.to_s.to_java_bytes)
      else
        p.add(family, qualifier, value.to_s.to_java_bytes)
      end
      @table.put(p)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a cell
    def _delete_internal(row, column, timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
      _deleteall_internal(row, column, timestamp)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a row
    def _deleteall_internal(row, column = nil, timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
      raise ArgumentError, "Row Not Found" if _get_internal(row).nil?
      d = org.apache.hadoop.hbase.client.Delete.new(row.to_s.to_java_bytes, timestamp)
      if column
        family, qualifier = parse_column_name(column)
        d.deleteColumns(family, qualifier, timestamp)
      end
      @table.delete(d)
    end

    #----------------------------------------------------------------------------------------------
    # Increment a counter atomically
    def _incr_internal(row, column, value = nil)
      value ||= 1
      family, qualifier = parse_column_name(column)
      if qualifier.nil?
	  raise ArgumentError, "Failed to provide both column family and column qualifier for incr"
      end
      @table.incrementColumnValue(row.to_s.to_java_bytes, family, qualifier, value)
    end

    #----------------------------------------------------------------------------------------------
    # Count rows in a table
    def _count_internal(interval = 1000, caching_rows = 10)
      # We can safely set scanner caching with the first key only filter
      scan = org.apache.hadoop.hbase.client.Scan.new
      scan.cache_blocks = false
      scan.caching = caching_rows
      scan.setFilter(org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter.new)

      # Run the scanner
      scanner = @table.getScanner(scan)
      count = 0
      iter = scanner.iterator

      # Iterate results
      while iter.hasNext
        row = iter.next
        count += 1
        next unless (block_given? && count % interval == 0)
        # Allow command modules to visualize counting process
        yield(count, 
              org.apache.hadoop.hbase.util.Bytes::toStringBinary(row.getRow))
      end

      # Return the counter
      return count
    end

    #----------------------------------------------------------------------------------------------
    # Get from table
    def _get_internal(row, *args)
      get = org.apache.hadoop.hbase.client.Get.new(row.to_s.to_java_bytes)
      maxlength = -1
      @converters.clear()
      
      # Normalize args
      args = args.first if args.first.kind_of?(Hash)
      if args.kind_of?(String) || args.kind_of?(Array)
        columns = [ args ].flatten.compact
        args = { COLUMNS => columns }
      end

      #
      # Parse arguments
      #
      unless args.kind_of?(Hash)
        raise ArgumentError, "Failed parse of of #{args.inspect}, #{args.class}"
      end

      # Get maxlength parameter if passed
      maxlength = args.delete(MAXLENGTH) if args[MAXLENGTH]
      filter = args.delete(FILTER) if args[FILTER]

      unless args.empty?
        columns = args[COLUMN] || args[COLUMNS]
        if args[VERSIONS]
          vers = args[VERSIONS]
        else
          vers = 1
        end
        if columns
          # Normalize types, convert string to an array of strings
          columns = [ columns ] if columns.is_a?(String)

          # At this point it is either an array or some unsupported stuff
          unless columns.kind_of?(Array)
            raise ArgumentError, "Failed parse column argument type #{args.inspect}, #{args.class}"
          end

          # Get each column name and add it to the filter
          columns.each do |column|
            family, qualifier = parse_column_name(column.to_s)
            if qualifier
              get.addColumn(family, qualifier)
            else
              get.addFamily(family)
            end
          end

          # Additional params
          get.setMaxVersions(vers)
          get.setTimeStamp(args[TIMESTAMP]) if args[TIMESTAMP]
          get.setTimeRange(args[TIMERANGE][0], args[TIMERANGE][1]) if args[TIMERANGE]
        else
          # May have passed TIMESTAMP and row only; wants all columns from ts.
          unless ts = args[TIMESTAMP] || tr = args[TIMERANGE]
            raise ArgumentError, "Failed parse of #{args.inspect}, #{args.class}"
          end

          get.setMaxVersions(vers)
          # Set the timestamp/timerange
          get.setTimeStamp(ts.to_i) if args[TIMESTAMP]
          get.setTimeRange(args[TIMERANGE][0], args[TIMERANGE][1]) if args[TIMERANGE]
        end
      end

      unless filter.class == String
        get.setFilter(filter)
      else
        get.setFilter(org.apache.hadoop.hbase.filter.ParseFilter.new.parseFilterString(filter))
      end

      # Call hbase for the results
      result = @table.get(get)
      return nil if result.isEmpty

      # Print out results.  Result can be Cell or RowResult.
      res = {}
      result.list.each do |kv|
        family = String.from_java_bytes(kv.getFamily)
        qualifier = org.apache.hadoop.hbase.util.Bytes::toStringBinary(kv.getQualifier)

        column = "#{family}:#{qualifier}"
        value = to_string(column, kv, maxlength)

        if block_given?
          yield(column, value)
        else
          res[column] = value
        end
      end

      # If block given, we've yielded all the results, otherwise just return them
      return ((block_given?) ? nil : res)
    end

    #----------------------------------------------------------------------------------------------
    # Fetches and decodes a counter value from hbase
    def _get_counter_internal(row, column)
      family, qualifier = parse_column_name(column.to_s)
      # Format get request
      get = org.apache.hadoop.hbase.client.Get.new(row.to_s.to_java_bytes)
      get.addColumn(family, qualifier)
      get.setMaxVersions(1)

      # Call hbase
      result = @table.get(get)
      return nil if result.isEmpty

      # Fetch cell value
      cell = result.list[0]
      org.apache.hadoop.hbase.util.Bytes::toLong(cell.getValue)
    end

    #----------------------------------------------------------------------------------------------
    # Scans whole table or a range of keys and returns rows matching specific criteria
    def _scan_internal(args = {})
      unless args.kind_of?(Hash)
        raise ArgumentError, "Arguments should be a hash. Failed to parse #{args.inspect}, #{args.class}"
      end

      limit = args.delete("LIMIT") || -1
      maxlength = args.delete("MAXLENGTH") || -1
      @converters.clear()

      if args.any?
        filter = args["FILTER"]
        startrow = args["STARTROW"] || ''
        stoprow = args["STOPROW"]
        timestamp = args["TIMESTAMP"]
        columns = args["COLUMNS"] || args["COLUMN"] || []
        cache_blocks = args["CACHE_BLOCKS"] || true
        cache = args["CACHE"] || 0
        versions = args["VERSIONS"] || 1
        timerange = args[TIMERANGE]
        raw = args["RAW"] || false

        # Normalize column names
        columns = [columns] if columns.class == String
        unless columns.kind_of?(Array)
          raise ArgumentError.new("COLUMNS must be specified as a String or an Array")
        end

        scan = if stoprow
          org.apache.hadoop.hbase.client.Scan.new(startrow.to_java_bytes, stoprow.to_java_bytes)
        else
          org.apache.hadoop.hbase.client.Scan.new(startrow.to_java_bytes)
        end

        columns.each do |c| 
          family, qualifier = parse_column_name(c.to_s)
          if qualifier
            scan.addColumn(family, qualifier)
          else
            scan.addFamily(family)
          end
        end

        unless filter.class == String
          scan.setFilter(filter)
        else
          scan.setFilter(org.apache.hadoop.hbase.filter.ParseFilter.new.parseFilterString(filter))
        end

        scan.setTimeStamp(timestamp) if timestamp
        scan.setCacheBlocks(cache_blocks)
        scan.setCaching(cache) if cache > 0
        scan.setMaxVersions(versions) if versions > 1
        scan.setTimeRange(timerange[0], timerange[1]) if timerange
        scan.setRaw(raw)
      else
        scan = org.apache.hadoop.hbase.client.Scan.new
      end

      # Start the scanner
      scanner = @table.getScanner(scan)
      count = 0
      res = {}
      iter = scanner.iterator

      # Iterate results
      while iter.hasNext
        if limit > 0 && count >= limit
          break
        end

        row = iter.next
        key = org.apache.hadoop.hbase.util.Bytes::toStringBinary(row.getRow)

        row.list.each do |kv|
          family = String.from_java_bytes(kv.getFamily)
          qualifier = org.apache.hadoop.hbase.util.Bytes::toStringBinary(kv.getQualifier)

          column = "#{family}:#{qualifier}"
          cell = to_string(column, kv, maxlength)

          if block_given?
            yield(key, "column=#{column}, #{cell}")
          else
            res[key] ||= {}
            res[key][column] = cell
          end
        end

        # One more row processed
        count += 1
      end

      return ((block_given?) ? count : res)
    end

    #----------------------------
    # Add general administration utilities to the shell
    # each of the names below adds this method name to the table
    # by callling the corresponding method in the shell
    # Add single method utilities to the current class
    # Generally used for admin functions which just have one name and take the table name
    def self.add_admin_utils(*args)
      args.each do |method|
        define_method method do |*method_args|
          @shell.command(method, @name, *method_args)
        end
      end
    end

    #Add the following admin utilities to the table
    add_admin_utils :enable, :disable, :flush, :drop, :describe, :snapshot

    #----------------------------
    #give the general help for the table
    # or the named command
    def help (command = nil)
      #if there is a command, get the per-command help from the shell
      if command
        begin
          return @shell.help_command(command)
        rescue NoMethodError
          puts "Command \'#{command}\' does not exist. Please see general table help."
          return nil
        end
      end
      return @shell.help('table_help')
    end

    # Table to string
    def to_s
      cl = self.class()
      return "#{cl} - #{@name}"
    end

    # Standard ruby call to get the return value for an object
    # overriden here so we get sane semantics for printing a table on return
    def inspect
      to_s
    end

    #----------------------------------------------------------------------------------------
    # Helper methods

    # Returns a list of column names in the table
    def get_all_columns
      @table.table_descriptor.getFamilies.map do |family|
        "#{family.getNameAsString}:"
      end
    end

    # Checks if current table is one of the 'meta' tables
    def is_meta_table?
      tn = @table.table_name
      org.apache.hadoop.hbase.util.Bytes.equals(tn,
          org.apache.hadoop.hbase.TableName::META_TABLE_NAME.getName)
    end

    # Returns family and (when has it) qualifier for a column name
    def parse_column_name(column)
      split = org.apache.hadoop.hbase.KeyValue.parseColumn(column.to_java_bytes)
      set_converter(split) if split.length > 1
      return split[0], (split.length > 1) ? split[1] : nil
    end

    # Make a String of the passed kv
    # Intercept cells whose format we know such as the info:regioninfo in hbase:meta
    def to_string(column, kv, maxlength = -1)
      if is_meta_table?
        if column == 'info:regioninfo' or column == 'info:splitA' or column == 'info:splitB'
          hri = org.apache.hadoop.hbase.HRegionInfo.parseFromOrNull(kv.getValue)
          return "timestamp=%d, value=%s" % [kv.getTimestamp, hri.toString]
        end
        if column == 'info:serverstartcode'
          if kv.getValue.length > 0
            str_val = org.apache.hadoop.hbase.util.Bytes.toLong(kv.getValue)
          else
            str_val = org.apache.hadoop.hbase.util.Bytes.toStringBinary(kv.getValue)
          end
          return "timestamp=%d, value=%s" % [kv.getTimestamp, str_val]
        end
      end

      if kv.isDelete
        val = "timestamp=#{kv.getTimestamp}, type=#{org.apache.hadoop.hbase.KeyValue::Type::codeToType(kv.getType)}"
      else
        val = "timestamp=#{kv.getTimestamp}, value=#{convert(column, kv)}"
      end
      (maxlength != -1) ? val[0, maxlength] : val
    end
    
    def convert(column, kv)
      #use org.apache.hadoop.hbase.util.Bytes as the default class
      klazz_name = 'org.apache.hadoop.hbase.util.Bytes'
      #use org.apache.hadoop.hbase.util.Bytes::toStringBinary as the default convertor
      converter = 'toStringBinary'
      if @converters.has_key?(column)
        # lookup the CONVERTER for certain column - "cf:qualifier"
        matches = /c\((.+)\)\.(.+)/.match(@converters[column])
        if matches.nil?
          # cannot match the pattern of 'c(className).functionname'
          # use the default klazz_name
          converter = @converters[column] 
        else
          klazz_name = matches[1]
          converter = matches[2]
        end
      end
      method = eval(klazz_name).method(converter)
      return method.call(kv.getValue) # apply the converter
    end
    
    # if the column spec contains CONVERTER information, to get rid of :CONVERTER info from column pair.
    # 1. return back normal column pair as usual, i.e., "cf:qualifier[:CONVERTER]" to "cf" and "qualifier" only
    # 2. register the CONVERTER information based on column spec - "cf:qualifier"
    def set_converter(column)
      family = String.from_java_bytes(column[0])
      parts = org.apache.hadoop.hbase.KeyValue.parseColumn(column[1])
      if parts.length > 1
        @converters["#{family}:#{String.from_java_bytes(parts[0])}"] = String.from_java_bytes(parts[1])
        column[1] = parts[0]
      end
    end
  end
end
