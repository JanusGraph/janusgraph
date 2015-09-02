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

# Wrapper for org.apache.hadoop.hbase.client.Table

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

    def initialize(table, shell)
      @table = table
      @name = @table.getName().getNameAsString()
      @shell = shell
      @converters = Hash.new()
    end

    def close()
      @table.close()
    end

    # Note the below methods are prefixed with '_' to hide them from the average user, as
    # they will be much less likely to tab complete to the 'dangerous' internal method
    #----------------------------------------------------------------------------------------------

    # Put a cell 'value' at specified table/row/column
    def _put_internal(row, column, value, timestamp = nil, args = {})
      p = org.apache.hadoop.hbase.client.Put.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if args.any?
         attributes = args[ATTRIBUTES]
         set_attributes(p, attributes) if attributes
         visibility = args[VISIBILITY]
         set_cell_visibility(p, visibility) if visibility
         ttl = args[TTL]
         set_op_ttl(p, ttl) if ttl
      end
      #Case where attributes are specified without timestamp
      if timestamp.kind_of?(Hash)
      	timestamp.each do |k, v|
          if k == 'ATTRIBUTES'
            set_attributes(p, v)
          elsif k == 'VISIBILITY'
            set_cell_visibility(p, v)
          elsif k == "TTL"
            set_op_ttl(p, v)
          end
        end
        timestamp = nil
      end
      if timestamp
        p.add(family, qualifier, timestamp, value.to_s.to_java_bytes)
      else
        p.add(family, qualifier, value.to_s.to_java_bytes)
      end
      @table.put(p)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a cell
    def _delete_internal(row, column,
    			timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP, args = {})
      _deleteall_internal(row, column, timestamp, args)
    end

    #----------------------------------------------------------------------------------------------
    # Delete a row
    def _deleteall_internal(row, column = nil,
    		timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP, args = {})
      # delete operation doesn't need read permission. Retaining the read check for
      # meta table as a part of HBASE-5837.
      if is_meta_table?
        raise ArgumentError, "Row Not Found" if _get_internal(row).nil?
      end
      temptimestamp = timestamp
      if temptimestamp.kind_of?(Hash)
      	  timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP
      end
      d = org.apache.hadoop.hbase.client.Delete.new(row.to_s.to_java_bytes, timestamp)
      if temptimestamp.kind_of?(Hash)
      	temptimestamp.each do |k, v|
      	  if v.kind_of?(String)
      	  	set_cell_visibility(d, v) if v
      	  end
      	 end
      end
      if args.any?
         visibility = args[VISIBILITY]
         set_cell_visibility(d, visibility) if visibility
      end
      if column
        family, qualifier = parse_column_name(column)
        d.deleteColumns(family, qualifier, timestamp)
      end
      @table.delete(d)
    end

    #----------------------------------------------------------------------------------------------
    # Increment a counter atomically
    def _incr_internal(row, column, value = nil, args={})
      if value.kind_of?(Hash)
      	value = 1
      end
      value ||= 1
      incr = org.apache.hadoop.hbase.client.Increment.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if qualifier.nil?
	  	raise ArgumentError, "Failed to provide both column family and column qualifier for incr"
      end
      if args.any?
      	attributes = args[ATTRIBUTES]
      	visibility = args[VISIBILITY]
        set_attributes(incr, attributes) if attributes
        set_cell_visibility(incr, visibility) if visibility
        ttl = args[TTL]
        set_op_ttl(incr, ttl) if ttl
      end
      incr.addColumn(family, qualifier, value)
      result = @table.increment(incr)
      return nil if result.isEmpty

      # Fetch cell value
      cell = result.listCells[0]
      org.apache.hadoop.hbase.util.Bytes::toLong(cell.getValue)
    end

    #----------------------------------------------------------------------------------------------
    # appends the value atomically
    def _append_internal(row, column, value, args={})
      append = org.apache.hadoop.hbase.client.Append.new(row.to_s.to_java_bytes)
      family, qualifier = parse_column_name(column)
      if qualifier.nil?
	  	raise ArgumentError, "Failed to provide both column family and column qualifier for append"
      end
      if args.any?
      	attributes = args[ATTRIBUTES]
      	visibility = args[VISIBILITY]
        set_attributes(append, attributes) if attributes
        set_cell_visibility(append, visibility) if visibility
        ttl = args[TTL]
        set_op_ttl(append, ttl) if ttl
      end
      append.add(family, qualifier, value.to_s.to_java_bytes)
      @table.append(append)
    end

    #----------------------------------------------------------------------------------------------
    # Count rows in a table
    def _count_internal(interval = 1000, caching_rows = 10)
      # We can safely set scanner caching with the first key only filter
      scan = org.apache.hadoop.hbase.client.Scan.new
      scan.setCacheBlocks(false)
      scan.setCaching(caching_rows)
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

      scanner.close()
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
      attributes = args[ATTRIBUTES]
      authorizations = args[AUTHORIZATIONS]
      consistency = args.delete(CONSISTENCY) if args[CONSISTENCY]
      replicaId = args.delete(REGION_REPLICA_ID) if args[REGION_REPLICA_ID]
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
          if attributes
          	 set_attributes(get, attributes)
          elsif authorizations
          	 set_authorizations(get, authorizations)
          else
          	# May have passed TIMESTAMP and row only; wants all columns from ts.
          	unless ts = args[TIMESTAMP] || tr = args[TIMERANGE]
            	raise ArgumentError, "Failed parse of #{args.inspect}, #{args.class}"
          	end
          end

          get.setMaxVersions(vers)
          # Set the timestamp/timerange
          get.setTimeStamp(ts.to_i) if args[TIMESTAMP]
          get.setTimeRange(args[TIMERANGE][0], args[TIMERANGE][1]) if args[TIMERANGE]
        end
        set_attributes(get, attributes) if attributes
        set_authorizations(get, authorizations) if authorizations
      end

      unless filter.class == String
        get.setFilter(filter)
      else
        get.setFilter(org.apache.hadoop.hbase.filter.ParseFilter.new.parseFilterString(filter))
      end

      get.setConsistency(org.apache.hadoop.hbase.client.Consistency.valueOf(consistency)) if consistency
      get.setReplicaId(replicaId) if replicaId

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

    def _hash_to_scan(args)
      if args.any?
        filter = args["FILTER"]
        startrow = args["STARTROW"] || ''
        stoprow = args["STOPROW"]
        timestamp = args["TIMESTAMP"]
        columns = args["COLUMNS"] || args["COLUMN"] || []
        # If CACHE_BLOCKS not set, then default 'true'.
        cache_blocks = args["CACHE_BLOCKS"].nil? ? true: args["CACHE_BLOCKS"]
        cache = args["CACHE"] || 0
        reversed = args["REVERSED"] || false
        versions = args["VERSIONS"] || 1
        timerange = args[TIMERANGE]
        raw = args["RAW"] || false
        attributes = args[ATTRIBUTES]
        authorizations = args[AUTHORIZATIONS]
        consistency = args[CONSISTENCY]
        # Normalize column names
        columns = [columns] if columns.class == String
        limit = args["LIMIT"] || -1
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
        scan.setReversed(reversed)
        scan.setCaching(cache) if cache > 0
        scan.setMaxVersions(versions) if versions > 1
        scan.setTimeRange(timerange[0], timerange[1]) if timerange
        scan.setRaw(raw)
        scan.setCaching(limit) if limit > 0
        set_attributes(scan, attributes) if attributes
        set_authorizations(scan, authorizations) if authorizations
        scan.setConsistency(org.apache.hadoop.hbase.client.Consistency.valueOf(consistency)) if consistency
      else
        scan = org.apache.hadoop.hbase.client.Scan.new
      end

      scan
    end

    def _get_scanner(args)
      @table.getScanner(_hash_to_scan(args))
    end

    #----------------------------------------------------------------------------------------------
    # Scans whole table or a range of keys and returns rows matching specific criteria
    def _scan_internal(args = {})
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)

      limit = args["LIMIT"] || -1
      maxlength = args.delete("MAXLENGTH") || -1
      count = 0
      res = {}

      @converters.clear()

      # Start the scanner
      scanner = @table.getScanner(_hash_to_scan(args))
      iter = scanner.iterator

      # Iterate results
      while iter.hasNext
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
        if limit > 0 && count >= limit
          # If we reached the limit, exit before the next call to hasNext
          break
        end
      end

      scanner.close()
      return ((block_given?) ? count : res)
    end

     # Apply OperationAttributes to puts/scans/gets
    def set_attributes(oprattr, attributes)
      raise(ArgumentError, "Attributes must be a Hash type") unless attributes.kind_of?(Hash)
      for k,v in attributes
        v = v.to_s unless v.nil?
        oprattr.setAttribute(k.to_s, v.to_java_bytes)
      end
    end

    def set_cell_permissions(op, permissions)
      raise(ArgumentError, "Permissions must be a Hash type") unless permissions.kind_of?(Hash)
      map = java.util.HashMap.new
      permissions.each do |user,perms|
        map.put(user.to_s, org.apache.hadoop.hbase.security.access.Permission.new(
          perms.to_java_bytes))
      end
      op.setACL(map)
    end

    def set_cell_visibility(oprattr, visibility)
      oprattr.setCellVisibility(
        org.apache.hadoop.hbase.security.visibility.CellVisibility.new(
          visibility.to_s))
    end

    def set_authorizations(oprattr, authorizations)
      raise(ArgumentError, "Authorizations must be a Array type") unless authorizations.kind_of?(Array)
      auths = [ authorizations ].flatten.compact
      oprattr.setAuthorizations(
        org.apache.hadoop.hbase.security.visibility.Authorizations.new(
          auths.to_java(:string)))
    end

    def set_op_ttl(op, ttl)
      op.setTTL(ttl.to_java(:long))
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
      org.apache.hadoop.hbase.TableName::META_TABLE_NAME.equals(@table.getName())
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
