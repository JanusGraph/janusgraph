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
java_import java.util.Arrays
java_import org.apache.hadoop.hbase.util.Pair
java_import org.apache.hadoop.hbase.util.RegionSplitter
java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.ServerName
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos::SnapshotDescription

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    include HBaseConstants

    def initialize(admin, formatter)
      @admin = admin
      @connection = @admin.getConnection()
      @formatter = formatter
    end

    def close
      @admin.close
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in hbase
    def list(regex = ".*")
      @admin.listTables(regex).map { |t| t.getNameAsString }
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region flush
    def flush(table_or_region_name)
      @admin.flush(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region or column family compaction
    def compact(table_or_region_name, family = nil)
      if family == nil
        @admin.compact(table_or_region_name)
      else
        # We are compacting a column family within a region.
        @admin.compact(table_or_region_name, family)
      end
    end

    # Requests to compact all regions on the regionserver
    def compact_regionserver(servername, major = false)
      @admin.compactRegionServer(ServerName.valueOf(servername), major)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region or column family major compaction
    def major_compact(table_or_region_name, family = nil)
      if family == nil
        @admin.majorCompact(table_or_region_name)
      else
        # We are major compacting a column family within a region or table.
        @admin.majorCompact(table_or_region_name, family)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Requests a regionserver's WAL roll
    def wal_roll(server_name)
      @admin.rollWALWriter(ServerName.valueOf(server_name))
    end
    # TODO remove older hlog_roll version
    alias :hlog_roll :wal_roll

    #----------------------------------------------------------------------------------------------
    # Requests a table or region split
    def split(table_or_region_name, split_point)
      if split_point == nil
        @admin.split(table_or_region_name)
      else
        @admin.split(table_or_region_name, split_point)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Requests a cluster balance
    # Returns true if balancer ran
    def balancer()
      @admin.balancer()
    end

    #----------------------------------------------------------------------------------------------
    # Enable/disable balancer
    # Returns previous balancer switch setting.
    def balance_switch(enableDisable)
      @admin.setBalancerRunning(
        java.lang.Boolean::valueOf(enableDisable), java.lang.Boolean::valueOf(false))
    end

    #----------------------------------------------------------------------------------------------
    # Request a scan of the catalog table (for garbage collection)
    # Returns an int signifying the number of entries cleaned
    def catalogjanitor_run()
      @admin.runCatalogScan()
    end

    #----------------------------------------------------------------------------------------------
    # Enable/disable the catalog janitor
    # Returns previous catalog janitor switch setting.
    def catalogjanitor_switch(enableDisable)
      @admin.enableCatalogJanitor(java.lang.Boolean::valueOf(enableDisable))
    end

    #----------------------------------------------------------------------------------------------
    # Query on the catalog janitor state (enabled/disabled?)
    # Returns catalog janitor state (true signifies enabled).
    def catalogjanitor_enabled()
      @admin.isCatalogJanitorEnabled()
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table
    def enable(table_name)
      tableExists(table_name)
      return if enabled?(table_name)
      @admin.enableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Enables all tables matching the given regex
    def enable_all(regex)
      regex = regex.to_s
      @admin.enableTables(regex)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a table
    def disable(table_name)
      tableExists(table_name)
      return if disabled?(table_name)
      @admin.disableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Disables all tables matching the given regex
    def disable_all(regex)
      regex = regex.to_s
      @admin.disableTables(regex).map { |t| t.getTableName().getNameAsString }
    end

    #---------------------------------------------------------------------------------------------
    # Throw exception if table doesn't exist
    def tableExists(table_name)
      raise ArgumentError, "Table #{table_name} does not exist." unless exists?(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table disabled?
    def disabled?(table_name)
      @admin.isTableDisabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop(table_name)
      tableExists(table_name)
      raise ArgumentError, "Table #{table_name} is enabled. Disable it first." if enabled?(table_name)

      @admin.deleteTable(org.apache.hadoop.hbase.TableName.valueOf(table_name))
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop_all(regex)
      regex = regex.to_s
      failed  = @admin.deleteTables(regex).map { |t| t.getTableName().getNameAsString }
      return failed
    end

    #----------------------------------------------------------------------------------------------
    # Returns ZooKeeper status dump
    def zk_dump
      @zk_wrapper = org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.new(
        @admin.getConfiguration(),
       "admin",
        nil)
      zk = @zk_wrapper.getRecoverableZooKeeper().getZooKeeper()
      @zk_main = org.apache.zookeeper.ZooKeeperMain.new(zk)
      org.apache.hadoop.hbase.zookeeper.ZKUtil::dump(@zk_wrapper)
    end

    #----------------------------------------------------------------------------------------------
    # Creates a table
    def create(table_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact
      has_columns = false

      # Start defining the table
      htd = org.apache.hadoop.hbase.HTableDescriptor.new(org.apache.hadoop.hbase.TableName.valueOf(table_name))
      splits = nil
      # Args are either columns or splits, add them to the table definition
      # TODO: add table options support
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end

        # First, handle all the cases where arg is a column family.
        if arg.kind_of?(String) or arg.has_key?(NAME)
          # If the arg is a string, default action is to add a column to the table.
          # If arg has a name, it must also be a column descriptor.
          descriptor = hcd(arg, htd);
          # Warn if duplicate columns are added
          if htd.hasFamily(descriptor.getName)
            puts "Family '" + descriptor.getNameAsString() + "' already exists, the old one will be replaced"
            htd.modifyFamily(descriptor)
          else
            htd.addFamily(descriptor)
          end
          has_columns = true
          next
        end
        if arg.has_key?(REGION_REPLICATION)
          region_replication = JInteger.valueOf(arg.delete(REGION_REPLICATION))
          htd.setRegionReplication(region_replication)
        end

        # Get rid of the "METHOD", which is deprecated for create.
        # We'll do whatever it used to do below if it's table_att.
        if (method = arg.delete(METHOD))
            raise(ArgumentError, "table_att is currently the only supported method") unless method == 'table_att'
        end

        # The hash is not a column family. Figure out what's in it.
        # First, handle splits.
        if arg.has_key?(SPLITS_FILE)
          splits_file = arg.delete(SPLITS_FILE)
          unless File.exist?(splits_file)
            raise(ArgumentError, "Splits file #{splits_file} doesn't exist")
          end
          arg[SPLITS] = []
          File.foreach(splits_file) do |line|
            arg[SPLITS].push(line.strip())
          end
          htd.setValue(SPLITS_FILE, arg[SPLITS_FILE])
        end

        if arg.has_key?(SPLITS)
          splits = Java::byte[][arg[SPLITS].size].new
          idx = 0
          arg.delete(SPLITS).each do |split|
            splits[idx] = org.apache.hadoop.hbase.util.Bytes.toBytesBinary(split)
            idx = idx + 1
          end
        elsif arg.has_key?(NUMREGIONS) or arg.has_key?(SPLITALGO)
          # deprecated region pre-split API; if one of the above is specified, will be ignored.
          raise(ArgumentError, "Number of regions must be specified") unless arg.has_key?(NUMREGIONS)
          raise(ArgumentError, "Split algorithm must be specified") unless arg.has_key?(SPLITALGO)
          raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
          num_regions = arg.delete(NUMREGIONS)
          split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg.delete(SPLITALGO))
          splits = split_algo.split(JInteger.valueOf(num_regions))
        end

        # Done with splits; apply formerly-table_att parameters.
        htd.setOwnerString(arg.delete(OWNER)) if arg[OWNER]
        htd.setMaxFileSize(JLong.valueOf(arg.delete(MAX_FILESIZE))) if arg[MAX_FILESIZE]
        htd.setReadOnly(JBoolean.valueOf(arg.delete(READONLY))) if arg[READONLY]
        htd.setCompactionEnabled(JBoolean.valueOf(arg[COMPACTION_ENABLED])) if arg[COMPACTION_ENABLED]
        htd.setMemStoreFlushSize(JLong.valueOf(arg.delete(MEMSTORE_FLUSHSIZE))) if arg[MEMSTORE_FLUSHSIZE]
        # DEFERRED_LOG_FLUSH is deprecated and was replaced by DURABILITY.  To keep backward compatible, it still exists.
        # However, it has to be set before DURABILITY so that DURABILITY could overwrite if both args are set
        if arg.include?(DEFERRED_LOG_FLUSH)
          if arg.delete(DEFERRED_LOG_FLUSH).to_s.upcase == "TRUE"
            htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf("ASYNC_WAL"))
          else
            htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf("SYNC_WAL"))
          end
        end
        htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf(arg.delete(DURABILITY))) if arg[DURABILITY]
        set_user_metadata(htd, arg.delete(METADATA)) if arg[METADATA]
        set_descriptor_config(htd, arg.delete(CONFIGURATION)) if arg[CONFIGURATION]

        arg.each_key do |ignored_key|
          puts("An argument ignored (unknown or overridden): %s" % [ ignored_key ])
        end
      end

      # Fail if no column families defined
      raise(ArgumentError, "Table must have at least one column family") if !has_columns

      if splits.nil?
        # Perform the create table call
        @admin.createTable(htd)
      else
        # Perform the create table call
        @admin.createTable(htd, splits)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Closes a region.
    # If server name is nil, we presume region_name is full region name (HRegionInfo.getRegionName).
    # If server name is not nil, we presume it is the region's encoded name (HRegionInfo.getEncodedName)
    def close_region(region_name, server)
      if (server == nil || !closeEncodedRegion?(region_name, server))
      	@admin.closeRegion(region_name, server)
      end
    end

    #----------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------
    # Assign a region
    def assign(region_name)
      @admin.assign(region_name.to_java_bytes)
    end

    #----------------------------------------------------------------------------------------------
    # Unassign a region
    def unassign(region_name, force)
      @admin.unassign(region_name.to_java_bytes, java.lang.Boolean::valueOf(force))
    end

    #----------------------------------------------------------------------------------------------
    # Move a region
    def move(encoded_region_name, server = nil)
      @admin.move(encoded_region_name.to_java_bytes, server ? server.to_java_bytes: nil)
    end

    #----------------------------------------------------------------------------------------------
    # Merge two regions
    def merge_region(encoded_region_a_name, encoded_region_b_name, force)
      @admin.mergeRegions(encoded_region_a_name.to_java_bytes, encoded_region_b_name.to_java_bytes, java.lang.Boolean::valueOf(force))
    end

    #----------------------------------------------------------------------------------------------
    # Returns table's structure description
    def describe(table_name)
      @admin.getTableDescriptor(TableName.valueOf(table_name)).to_s
    end

    def get_column_families(table_name)
      @admin.getTableDescriptor(TableName.valueOf(table_name)).getColumnFamilies()
    end

    def get_table_attributes(table_name)
      @admin.getTableDescriptor(TableName.valueOf(table_name)).toStringTableAttributes
    end

    #----------------------------------------------------------------------------------------------
    # Truncates table (deletes all records by recreating the table)
    def truncate(table_name, conf = @conf)
      table_description = @admin.getTableDescriptor(TableName.valueOf(table_name))
      raise ArgumentError, "Table #{table_name} is not enabled. Enable it first." unless enabled?(table_name)
      yield 'Disabling table...' if block_given?
      @admin.disableTable(table_name)

      begin
        yield 'Truncating table...' if block_given?
        @admin.truncateTable(org.apache.hadoop.hbase.TableName.valueOf(table_name), false)
      rescue => e
        # Handle the compatibility case, where the truncate method doesn't exists on the Master
        raise e unless e.respond_to?(:cause) && e.cause != nil
        rootCause = e.cause
        if rootCause.kind_of?(org.apache.hadoop.hbase.DoNotRetryIOException) then
          # Handle the compatibility case, where the truncate method doesn't exists on the Master
          yield 'Dropping table...' if block_given?
          @admin.deleteTable(org.apache.hadoop.hbase.TableName.valueOf(table_name))

          yield 'Creating table...' if block_given?
          @admin.createTable(table_description)
        else
          raise e
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    # Truncates table while maintaing region boundaries (deletes all records by recreating the table)
    def truncate_preserve(table_name, conf = @conf)
      h_table = @connection.getTable(TableName.valueOf(table_name))
      locator = @connection.getRegionLocator(TableName.valueOf(table_name))
      splits = locator.getAllRegionLocations().
          map{|i| Bytes.toString(i.getRegionInfo().getStartKey)}.
          delete_if{|k| k == ""}.to_java :String
      locator.close()

      table_description = @admin.getTableDescriptor(TableName.valueOf(table_name))
      yield 'Disabling table...' if block_given?
      disable(table_name)

      begin
        yield 'Truncating table...' if block_given?
        @admin.truncateTable(org.apache.hadoop.hbase.TableName.valueOf(table_name), true)
      rescue => e
        # Handle the compatibility case, where the truncate method doesn't exists on the Master
        raise e unless e.respond_to?(:cause) && e.cause != nil
        rootCause = e.cause
        if rootCause.kind_of?(org.apache.hadoop.hbase.DoNotRetryIOException) then
          # Handle the compatibility case, where the truncate method doesn't exists on the Master
          yield 'Dropping table...' if block_given?
          @admin.deleteTable(org.apache.hadoop.hbase.TableName.valueOf(table_name))

          yield 'Creating table with region boundaries...' if block_given?
          @admin.createTable(table_description, splits)
        else
          raise e
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    # Check the status of alter command (number of regions reopened)
    def alter_status(table_name)
      # Table name should be a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Table should exist
      raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

      status = Pair.new()
      begin
        status = @admin.getAlterStatus(org.apache.hadoop.hbase.TableName.valueOf(table_name))
        if status.getSecond() != 0
          puts "#{status.getSecond() - status.getFirst()}/#{status.getSecond()} regions updated."
        else
          puts "All regions updated."
        end
	      sleep 1
      end while status != nil && status.getFirst() != 0
      puts "Done."
    end

    #----------------------------------------------------------------------------------------------
    # Change table structure or table options
    def alter(table_name, wait = true, *args)
      # Table name should be a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Table should exist
      raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

      # There should be at least one argument
      raise(ArgumentError, "There should be at least one argument but the table name") if args.empty?

      # Get table descriptor
      htd = @admin.getTableDescriptor(TableName.valueOf(table_name))

      # Process all args
      args.each do |arg|


        # Normalize args to support column name only alter specs
        arg = { NAME => arg } if arg.kind_of?(String)

        # Normalize args to support shortcut delete syntax
        arg = { METHOD => 'delete', NAME => arg['delete'] } if arg['delete']

        # There are 3 possible options.
        # 1) Column family spec. Distinguished by having a NAME and no METHOD.
        method = arg.delete(METHOD)
        if method == nil and arg.has_key?(NAME)
          descriptor = hcd(arg, htd)
          column_name = descriptor.getNameAsString

          # If column already exist, then try to alter it. Create otherwise.
          if htd.hasFamily(column_name.to_java_bytes)
            @admin.modifyColumn(table_name, descriptor)
          else
            @admin.addColumn(table_name, descriptor)
          end

          if wait == true
            puts "Updating all regions with the new schema..."
            alter_status(table_name)
          end

          # We bypass descriptor when adding column families; refresh it to apply other args correctly.
          htd = @admin.getTableDescriptor(TableName.valueOf(table_name))
          next
        end

        # 2) Method other than table_att, with some args.
        name = arg.delete(NAME)
        if method != nil and method != "table_att"
          # Delete column family
          if method == "delete"
            raise(ArgumentError, "NAME parameter missing for delete method") unless name
            @admin.deleteColumn(table_name, name)
          # Unset table attributes
          elsif method == "table_att_unset"
            raise(ArgumentError, "NAME parameter missing for table_att_unset method") unless name
            if name.kind_of?(Array)
              name.each do |key|
                if (htd.getValue(key) == nil)
                  raise ArgumentError, "Could not find attribute: #{key}"
                end
                htd.remove(key)
              end
            else
              if (htd.getValue(name) == nil)
                raise ArgumentError, "Could not find attribute: #{name}"
              end
              htd.remove(name)
            end
            @admin.modifyTable(table_name.to_java_bytes, htd)
          # Unknown method
          else
            raise ArgumentError, "Unknown method: #{method}"
          end

          arg.each_key do |unknown_key|
            puts("Unknown argument ignored: %s" % [unknown_key])
          end

          if wait == true
            puts "Updating all regions with the new schema..."
            alter_status(table_name)
          end

          if method == "delete"
            # We bypass descriptor when deleting column families; refresh it to apply other args correctly.
            htd = @admin.getTableDescriptor(TableName.valueOf(table_name))
          end
          next
        end

        # 3) Some args for the table, optionally with METHOD => table_att (deprecated)
        raise(ArgumentError, "NAME argument in an unexpected place") if name
        htd.setOwnerString(arg.delete(OWNER)) if arg[OWNER]
        htd.setMaxFileSize(JLong.valueOf(arg.delete(MAX_FILESIZE))) if arg[MAX_FILESIZE]
        htd.setReadOnly(JBoolean.valueOf(arg.delete(READONLY))) if arg[READONLY]
        htd.setCompactionEnabled(JBoolean.valueOf(arg[COMPACTION_ENABLED])) if arg[COMPACTION_ENABLED]
        htd.setMemStoreFlushSize(JLong.valueOf(arg.delete(MEMSTORE_FLUSHSIZE))) if arg[MEMSTORE_FLUSHSIZE]
        # DEFERRED_LOG_FLUSH is deprecated and was replaced by DURABILITY.  To keep backward compatible, it still exists.
        # However, it has to be set before DURABILITY so that DURABILITY could overwrite if both args are set
        if arg.include?(DEFERRED_LOG_FLUSH)
          if arg.delete(DEFERRED_LOG_FLUSH).to_s.upcase == "TRUE"
            htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf("ASYNC_WAL"))
          else
            htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf("SYNC_WAL"))
          end
        end
        htd.setDurability(org.apache.hadoop.hbase.client.Durability.valueOf(arg.delete(DURABILITY))) if arg[DURABILITY]
        htd.setRegionReplication(JInteger.valueOf(arg.delete(REGION_REPLICATION))) if arg[REGION_REPLICATION]
        set_user_metadata(htd, arg.delete(METADATA)) if arg[METADATA]
        set_descriptor_config(htd, arg.delete(CONFIGURATION)) if arg[CONFIGURATION]

        # set a coprocessor attribute
        valid_coproc_keys = []
        if arg.kind_of?(Hash)
          arg.each do |key, value|
            k = String.new(key) # prepare to strip
            k.strip!

            if (k =~ /coprocessor/i)
              v = String.new(value)
              v.strip!
              htd.addCoprocessor(v)
              valid_coproc_keys << key
            end
          end

          valid_coproc_keys.each do |key|
            arg.delete(key)
          end

          @admin.modifyTable(table_name.to_java_bytes, htd)

          arg.each_key do |unknown_key|
            puts("Unknown argument ignored: %s" % [unknown_key])
          end

          if wait == true
            puts "Updating all regions with the new schema..."
            alter_status(table_name)
          end
          next
        end
      end
    end

    def status(format, type)
      status = @admin.getClusterStatus()
      if format == "detailed"
        puts("version %s" % [ status.getHBaseVersion() ])
        # Put regions in transition first because usually empty
        puts("%d regionsInTransition" % status.getRegionsInTransition().size())
        for k, v in status.getRegionsInTransition()
          puts("    %s" % [v])
        end
        master_coprocs = java.util.Arrays.toString(@admin.getMasterCoprocessors())
        if master_coprocs != nil
          puts("master coprocessors: %s" % master_coprocs)
        end
        puts("%d live servers" % [ status.getServersSize() ])
        for server in status.getServers()
          puts("    %s:%d %d" % \
            [ server.getHostname(), server.getPort(), server.getStartcode() ])
          puts("        %s" % [ status.getLoad(server).toString() ])
          for name, region in status.getLoad(server).getRegionsLoad()
            puts("        %s" % [ region.getNameAsString().dump ])
            puts("            %s" % [ region.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
      elsif format == "replication"
        #check whether replication is enabled or not
        if (!@admin.getConfiguration().getBoolean(org.apache.hadoop.hbase.HConstants::REPLICATION_ENABLE_KEY, 
          org.apache.hadoop.hbase.HConstants::REPLICATION_ENABLE_DEFAULT))
          puts("Please enable replication first.")
        else
          puts("version %s" % [ status.getHBaseVersion() ])
          puts("%d live servers" % [ status.getServersSize() ])
          for server in status.getServers()
            sl = status.getLoad(server)
            rSinkString   = "       SINK  :"
            rSourceString = "       SOURCE:"
            rLoadSink = sl.getReplicationLoadSink()
            rSinkString << " AgeOfLastAppliedOp=" + rLoadSink.getAgeOfLastAppliedOp().to_s
            rSinkString << ", TimeStampsOfLastAppliedOp=" + 
			    (java.util.Date.new(rLoadSink.getTimeStampsOfLastAppliedOp())).toString()
            rLoadSourceList = sl.getReplicationLoadSourceList()
            index = 0
            while index < rLoadSourceList.size()
              rLoadSource = rLoadSourceList.get(index)
              rSourceString << " PeerID=" + rLoadSource.getPeerID()
              rSourceString << ", AgeOfLastShippedOp=" + rLoadSource.getAgeOfLastShippedOp().to_s
              rSourceString << ", SizeOfLogQueue=" + rLoadSource.getSizeOfLogQueue().to_s
              rSourceString << ", TimeStampsOfLastShippedOp=" + 
			      (java.util.Date.new(rLoadSource.getTimeStampOfLastShippedOp())).toString()
              rSourceString << ", Replication Lag=" + rLoadSource.getReplicationLag().to_s
              index = index + 1
            end
            puts("    %s:" %
            [ server.getHostname() ])
            if type.casecmp("SOURCE") == 0
              puts("%s" % rSourceString)
            elsif type.casecmp("SINK") == 0
              puts("%s" % rSinkString)
            else
              puts("%s" % rSourceString)
              puts("%s" % rSinkString)
            end
          end
        end
      elsif format == "simple"
        load = 0
        regions = 0
        puts("%d live servers" % [ status.getServersSize() ])
        for server in status.getServers()
          puts("    %s:%d %d" % \
            [ server.getHostname(), server.getPort(), server.getStartcode() ])
          puts("        %s" % [ status.getLoad(server).toString() ])
          load += status.getLoad(server).getNumberOfRequests()
          regions += status.getLoad(server).getNumberOfRegions()
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
        puts("Aggregate load: %d, regions: %d" % [ load , regions ] )
      else
        puts "#{status.getServersSize} servers, #{status.getDeadServers} dead, #{'%.4f' % status.getAverageLoad} average load"
      end
    end

    #----------------------------------------------------------------------------------------------
    #
    # Helper methods
    #

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table enabled
    def enabled?(table_name)
      @admin.isTableEnabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    #Is supplied region name is encoded region name
    def closeEncodedRegion?(region_name, server)
       @admin.closeRegionWithEncodedRegionName(region_name, server)
    end

    #----------------------------------------------------------------------------------------------
    # Return a new HColumnDescriptor made of passed args
    def hcd(arg, htd)
      # String arg, single parameter constructor
      return org.apache.hadoop.hbase.HColumnDescriptor.new(arg) if arg.kind_of?(String)

      raise(ArgumentError, "Column family #{arg} must have a name") unless name = arg.delete(NAME)

      family = htd.getFamily(name.to_java_bytes)
      # create it if it's a new family
      family ||= org.apache.hadoop.hbase.HColumnDescriptor.new(name.to_java_bytes)

      family.setBlockCacheEnabled(JBoolean.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE)
      family.setScope(JInteger.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE)
      family.setCacheDataOnWrite(JBoolean.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::CACHE_DATA_ON_WRITE))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::CACHE_DATA_ON_WRITE)
      family.setInMemory(JBoolean.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY)
      family.setTimeToLive(JInteger.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::TTL))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::TTL)
      family.setDataBlockEncoding(org.apache.hadoop.hbase.io.encoding.DataBlockEncoding.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING)
      family.setBlocksize(JInteger.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE)
      family.setMaxVersions(JInteger.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS)
      family.setMinVersions(JInteger.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS)
      family.setKeepDeletedCells(org.apache.hadoop.hbase.KeepDeletedCells.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS).to_s.upcase)) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS)
      family.setCompressTags(JBoolean.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESS_TAGS))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESS_TAGS)
      family.setPrefetchBlocksOnOpen(JBoolean.valueOf(arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::PREFETCH_BLOCKS_ON_OPEN))) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::PREFETCH_BLOCKS_ON_OPEN)
      family.setValue(COMPRESSION_COMPACT, arg.delete(COMPRESSION_COMPACT)) if arg.include?(COMPRESSION_COMPACT)
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER)
        bloomtype = arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER).upcase
        unless org.apache.hadoop.hbase.regionserver.BloomType.constants.include?(bloomtype)
          raise(ArgumentError, "BloomFilter type #{bloomtype} is not supported. Use one of " + org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.join(" "))
        else
          family.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.valueOf(bloomtype))
        end
      end
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION)
        compression = arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION).upcase
        unless org.apache.hadoop.hbase.io.compress.Compression::Algorithm.constants.include?(compression)
          raise(ArgumentError, "Compression #{compression} is not supported. Use one of " + org.apache.hadoop.hbase.io.compress.Compression::Algorithm.constants.join(" "))
        else
          family.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression::Algorithm.valueOf(compression))
        end
      end
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::ENCRYPTION)
        algorithm = arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::ENCRYPTION).upcase
        family.setEncryptionType(algorithm)
        if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::ENCRYPTION_KEY)
          key = org.apache.hadoop.hbase.io.crypto.Encryption.pbkdf128(
            arg.delete(org.apache.hadoop.hbase.HColumnDescriptor::ENCRYPTION_KEY))
          family.setEncryptionKey(org.apache.hadoop.hbase.security.EncryptionUtil.wrapKey(@conf, key,
            algorithm))
        end
      end

      set_user_metadata(family, arg.delete(METADATA)) if arg[METADATA]
      set_descriptor_config(family, arg.delete(CONFIGURATION)) if arg[CONFIGURATION]

      arg.each_key do |unknown_key|
        puts("Unknown argument ignored for column family %s: %s" % [name, unknown_key])
      end

      return family
    end

    #----------------------------------------------------------------------------------------------
    # Enables/disables a region by name
    def online(region_name, on_off)
      # Open meta table
      meta = @connection.getTable(org.apache.hadoop.hbase.TableName::META_TABLE_NAME)

      # Read region info
      # FIXME: fail gracefully if can't find the region
      region_bytes = region_name.to_java_bytes
      g = org.apache.hadoop.hbase.client.Get.new(region_bytes)
      g.addColumn(org.apache.hadoop.hbase.HConstants::CATALOG_FAMILY, org.apache.hadoop.hbase.HConstants::REGIONINFO_QUALIFIER)
      hri_bytes = meta.get(g).value

      # Change region status
      hri = org.apache.hadoop.hbase.util.Writables.getWritable(hri_bytes, org.apache.hadoop.hbase.HRegionInfo.new)
      hri.setOffline(on_off)

      # Write it back
      put = org.apache.hadoop.hbase.client.Put.new(region_bytes)
      put.add(org.apache.hadoop.hbase.HConstants::CATALOG_FAMILY, org.apache.hadoop.hbase.HConstants::REGIONINFO_QUALIFIER, org.apache.hadoop.hbase.util.Writables.getBytes(hri))
      meta.put(put)
    end
    # Apply user metadata to table/column descriptor
    def set_user_metadata(descriptor, metadata)
      raise(ArgumentError, "#{METADATA} must be a Hash type") unless metadata.kind_of?(Hash)
        for k,v in metadata
          v = v.to_s unless v.nil?
          descriptor.setValue(k, v)
        end
    end

    #----------------------------------------------------------------------------------------------
    # Take a snapshot of specified table
    def snapshot(table, snapshot_name, *args)
      if args.empty?
         @admin.snapshot(snapshot_name.to_java_bytes, table.to_java_bytes)
      else
         args.each do |arg|
            if arg[SKIP_FLUSH] == true
              @admin.snapshot(snapshot_name.to_java_bytes, table.to_java_bytes, SnapshotDescription::Type::SKIPFLUSH)
            else
               @admin.snapshot(snapshot_name.to_java_bytes, table.to_java_bytes)
            end
         end
      end
    end

    #----------------------------------------------------------------------------------------------
    # Restore specified snapshot
    def restore_snapshot(snapshot_name)
      @admin.restoreSnapshot(snapshot_name.to_java_bytes)
    end

    #----------------------------------------------------------------------------------------------
    # Create a new table by cloning the snapshot content
    def clone_snapshot(snapshot_name, table)
      @admin.cloneSnapshot(snapshot_name.to_java_bytes, table.to_java_bytes)
    end

    #----------------------------------------------------------------------------------------------
    # Delete specified snapshot
    def delete_snapshot(snapshot_name)
      @admin.deleteSnapshot(snapshot_name.to_java_bytes)
    end

    #----------------------------------------------------------------------------------------------
    # Deletes the snapshots matching the given regex
    def delete_all_snapshot(regex)
      @admin.deleteSnapshots(regex).to_a
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of snapshots
    def list_snapshot(regex = ".*")
      @admin.listSnapshots(regex).to_a
    end

    # Apply config specific to a table/column to its descriptor
    def set_descriptor_config(descriptor, config)
      raise(ArgumentError, "#{CONFIGURATION} must be a Hash type") unless config.kind_of?(Hash)
        for k,v in config
          v = v.to_s unless v.nil?
          descriptor.setConfiguration(k, v)
        end
    end

    #----------------------------------------------------------------------------------------------
    # Updates the configuration of one regionserver.
    def update_config(serverName)
      @admin.updateConfiguration(ServerName.valueOf(serverName));
    end

    #----------------------------------------------------------------------------------------------
    # Updates the configuration of all the regionservers.
    def update_all_config()
      @admin.updateConfiguration();
    end

    #----------------------------------------------------------------------------------------------
    # Returns namespace's structure description
    def describe_namespace(namespace_name)
      namespace = @admin.getNamespaceDescriptor(namespace_name)

      unless namespace.nil?
        return namespace.to_s
      end

      raise(ArgumentError, "Failed to find namespace named #{namespace_name}")
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of namespaces in hbase
    def list_namespace(regex = ".*")
      pattern = java.util.regex.Pattern.compile(regex)
      list = @admin.listNamespaceDescriptors.map { |ns| ns.getName }
      list.select {|s| pattern.match(s) }
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in namespace
    def list_namespace_tables(namespace_name)
      unless namespace_name.nil?
        return @admin.listTableNamesByNamespace(namespace_name).map { |t| t.getQualifierAsString() }
      end

      raise(ArgumentError, "Failed to find namespace named #{namespace_name}")
    end

    #----------------------------------------------------------------------------------------------
    # Creates a namespace
    def create_namespace(namespace_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Namespace name must be of type String") unless namespace_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact

      # Start defining the table
      nsb = org.apache.hadoop.hbase.NamespaceDescriptor::create(namespace_name)
      args.each do |arg|
        unless arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end
        for k,v in arg
          v = v.to_s unless v.nil?
          nsb.addConfiguration(k, v)
        end
      end
      @admin.createNamespace(nsb.build());
    end

    #----------------------------------------------------------------------------------------------
    # modify a namespace
    def alter_namespace(namespace_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Namespace name must be of type String") unless namespace_name.kind_of?(String)

      nsd = @admin.getNamespaceDescriptor(namespace_name)

      unless nsd
        raise(ArgumentError, "Namespace does not exist")
      end
      nsb = org.apache.hadoop.hbase.NamespaceDescriptor::create(nsd)

      # Flatten params array
      args = args.flatten.compact

      # Start defining the table
      args.each do |arg|
        unless arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash type")
        end
        method = arg[METHOD]
        if method == "unset"
          nsb.removeConfiguration(arg[NAME])
        elsif  method == "set"
          arg.delete(METHOD)
          for k,v in arg
            v = v.to_s unless v.nil?

            nsb.addConfiguration(k, v)
          end
        else
          raise(ArgumentError, "Unknown method #{method}")
        end
      end
      @admin.modifyNamespace(nsb.build());
    end


    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop_namespace(namespace_name)
      @admin.deleteNamespace(namespace_name)
    end

  end
end
