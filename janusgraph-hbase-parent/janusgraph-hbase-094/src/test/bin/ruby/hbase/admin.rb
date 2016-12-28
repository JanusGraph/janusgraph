#
# Copyright 2010 The Apache Software Foundation
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
java_import org.apache.hadoop.hbase.util.Pair
java_import org.apache.hadoop.hbase.util.RegionSplitter

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    include HBaseConstants

    def initialize(configuration, formatter)
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @conf = configuration
      @zk_wrapper = connection.getZooKeeperWatcher()
      zk = @zk_wrapper.getRecoverableZooKeeper().getZooKeeper()
      @zk_main = org.apache.zookeeper.ZooKeeperMain.new(zk)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in hbase
    def list(regex = ".*")
      begin
        # Use the old listTables API first for compatibility with older servers
        @admin.listTables(regex).map { |t| t.getNameAsString }
      rescue => e
        # listTables failed, try the new unprivileged getTableNames API if the cause was
        # an AccessDeniedException
        if e.cause.kind_of? org.apache.hadoop.ipc.RemoteException and e.cause.unwrapRemoteException().kind_of? org.apache.hadoop.hbase.security.AccessDeniedException
          @admin.getTableNames(regex)
        else
          # Not an access control failure, re-raise
          raise e
        end
      end
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
    # Requests a regionserver's HLog roll
    def hlog_roll(server_name)
      @admin.rollHLogWriter(server_name)
    end

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
      @admin.disableTables(regex).map { |t| t.getNameAsString }
    end

    #---------------------------------------------------------------------------------------------
    # Throw exception if table doesn't exist
    def tableExists(table_name)
      raise ArgumentError, "Table #{table_name} does not exist.'" unless exists?(table_name)
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
      raise ArgumentError, "Table #{table_name} is enabled. Disable it first.'" if enabled?(table_name)

      @admin.deleteTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop_all(regex)
      regex = regex.to_s
      failed  = @admin.deleteTables(regex).map { |t| t.getNameAsString }
      return failed
    end

    #----------------------------------------------------------------------------------------------
    # Returns ZooKeeper status dump
    def zk_dump
      org.apache.hadoop.hbase.zookeeper.ZKUtil::dump(@zk_wrapper)
    end

    #----------------------------------------------------------------------------------------------
    # Creates a table
    def create(table_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact

      # Fail if no column families defined
      raise(ArgumentError, "Table must have at least one column family") if args.empty?

      # Start defining the table
      htd = org.apache.hadoop.hbase.HTableDescriptor.new(table_name)
      splits = nil
      # Args are either columns or splits, add them to the table definition
      # TODO: add table options support
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end

        if arg.kind_of?(String)
          # the arg is a string, default action is to add a column to the table
          htd.addFamily(hcd(arg, htd))
        else
          # arg is a hash.  4 possibilities:
          if (arg.has_key?(SPLITS) or arg.has_key?(SPLITS_FILE))
            if arg.has_key?(SPLITS_FILE)
              unless File.exist?(arg[SPLITS_FILE])
                raise(ArgumentError, "Splits file #{arg[SPLITS_FILE]} doesn't exist")
              end
              arg[SPLITS] = []
              File.foreach(arg[SPLITS_FILE]) do |line|
                arg[SPLITS].push(line.strip())
              end
            end

            splits = Java::byte[][arg[SPLITS].size].new
            idx = 0
            arg[SPLITS].each do |split|
              splits[idx] = split.to_java_bytes
              idx = idx + 1
            end
          elsif (arg.has_key?(NUMREGIONS) or arg.has_key?(SPLITALGO))
            # (1) deprecated region pre-split API
            raise(ArgumentError, "Column family configuration should be specified in a separate clause") if arg.has_key?(NAME)
            raise(ArgumentError, "Number of regions must be specified") unless arg.has_key?(NUMREGIONS)
            raise(ArgumentError, "Split algorithm must be specified") unless arg.has_key?(SPLITALGO)
            raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
            num_regions = arg[NUMREGIONS]
            split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
            splits = split_algo.split(JInteger.valueOf(num_regions))
          elsif (method = arg.delete(METHOD))
            # (2) table_att modification
            raise(ArgumentError, "table_att is currently the only supported method") unless method == 'table_att'
            raise(ArgumentError, "NUMREGIONS & SPLITALGO must both be specified") unless arg.has_key?(NUMREGIONS) == arg.has_key?(split_algo)
            htd.setMaxFileSize(JLong.valueOf(arg[MAX_FILESIZE])) if arg[MAX_FILESIZE]
            htd.setReadOnly(JBoolean.valueOf(arg[READONLY])) if arg[READONLY]
            htd.setMemStoreFlushSize(JLong.valueOf(arg[MEMSTORE_FLUSHSIZE])) if arg[MEMSTORE_FLUSHSIZE]
            htd.setDeferredLogFlush(JBoolean.valueOf(arg[DEFERRED_LOG_FLUSH])) if arg[DEFERRED_LOG_FLUSH]
            htd.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT]) if arg[COMPRESSION_COMPACT]
            if arg[NUMREGIONS]
              raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
              num_regions = arg[NUMREGIONS]
              split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
              splits = split_algo.split(JInteger.valueOf(num_regions))
            end
            if arg[CONFIG]
              raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
              for k,v in arg[CONFIG]
                v = v.to_s unless v.nil?
                htd.setValue(k, v)
              end
            end
          else
            # (3) column family spec
            descriptor = hcd(arg, htd)
            htd.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT]) if arg[COMPRESSION_COMPACT]
            htd.addFamily(hcd(arg, htd))
          end
        end
      end

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
    # Returns table's structure description
    def describe(table_name)
      @admin.getTableDescriptor(table_name.to_java_bytes).to_s
    end

    #----------------------------------------------------------------------------------------------
    # Truncates table (deletes all records by recreating the table)
    def truncate(table_name, conf = @conf)
      h_table = org.apache.hadoop.hbase.client.HTable.new(conf, table_name)
      table_description = h_table.getTableDescriptor()
      raise ArgumentError, "Table #{table_name} is not enabled. Enable it first.'" unless enabled?(table_name)
      yield 'Disabling table...' if block_given?
      @admin.disableTable(table_name)

      yield 'Dropping table...' if block_given?
      @admin.deleteTable(table_name)

      yield 'Creating table...' if block_given?
      @admin.createTable(table_description)
    end

    # Check the status of alter command (number of regions reopened)
    def alter_status(table_name)
      # Table name should be a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Table should exist
      raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

      status = Pair.new()
      begin
        status = @admin.getAlterStatus(table_name.to_java_bytes)
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
      htd = @admin.getTableDescriptor(table_name.to_java_bytes)

      # Process all args
      args.each do |arg|
        # Normalize args to support column name only alter specs
        arg = { NAME => arg } if arg.kind_of?(String)

        # Normalize args to support shortcut delete syntax
        arg = { METHOD => 'delete', NAME => arg['delete'] } if arg['delete']

        # No method parameter, try to use the args as a column definition
        unless method = arg.delete(METHOD)
          # Note that we handle owner here, and also below (see (2)) as part of the "METHOD => 'table_att'" table attributes.
          # In other words, if OWNER is specified, then METHOD is set to table_att.
          #   alter 'tablename', {OWNER => 'username'} (that is, METHOD => 'table_att' is not specified).
          if arg[OWNER]
            htd.setOwnerString(arg[OWNER])
            @admin.modifyTable(table_name.to_java_bytes, htd)
            return
          end

          descriptor = hcd(arg, htd)

          if arg[COMPRESSION_COMPACT]
            descriptor.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT])
          end
          column_name = descriptor.getNameAsString

          # If column already exist, then try to alter it. Create otherwise.
          if htd.hasFamily(column_name.to_java_bytes)
            @admin.modifyColumn(table_name, descriptor)
            if wait == true
              puts "Updating all regions with the new schema..."
              alter_status(table_name)
            end
          else
            @admin.addColumn(table_name, descriptor)
            if wait == true
              puts "Updating all regions with the new schema..."
              alter_status(table_name)
            end
          end
          next
        end

        # Delete column family
        if method == "delete"
          raise(ArgumentError, "NAME parameter missing for delete method") unless arg[NAME]
          @admin.deleteColumn(table_name, arg[NAME])
          if wait == true
            puts "Updating all regions with the new schema..."
            alter_status(table_name)
          end
          next
        end

        # Change table attributes
        if method == "table_att"
          htd.setMaxFileSize(JLong.valueOf(arg[MAX_FILESIZE])) if arg[MAX_FILESIZE]
          htd.setReadOnly(JBoolean.valueOf(arg[READONLY])) if arg[READONLY]
          htd.setMemStoreFlushSize(JLong.valueOf(arg[MEMSTORE_FLUSHSIZE])) if arg[MEMSTORE_FLUSHSIZE]
          htd.setDeferredLogFlush(JBoolean.valueOf(arg[DEFERRED_LOG_FLUSH])) if arg[DEFERRED_LOG_FLUSH]
          # (2) Here, we handle the alternate syntax of ownership setting, where method => 'table_att' is specified.
          htd.setOwnerString(arg[OWNER]) if arg[OWNER]

          # set a coprocessor attribute
          if arg.kind_of?(Hash)
            arg.each do |key, value|
              k = String.new(key) # prepare to strip
              k.strip!

              if (k =~ /coprocessor/i)
                # validate coprocessor specs
                v = String.new(value)
                v.strip!
                if !(v =~ /^([^\|]*)\|([^\|]+)\|[\s]*([\d]*)[\s]*(\|.*)?$/)
                  raise ArgumentError, "Coprocessor value doesn't match spec: #{v}"
                end

                # generate a coprocessor ordinal by checking max id of existing cps
                maxId = 0
                htd.getValues().each do |k1, v1|
                  attrName = org.apache.hadoop.hbase.util.Bytes.toString(k1.get())
                  # a cp key is coprocessor$(\d)
                  if (attrName =~ /coprocessor\$(\d+)/i)
                    ids = attrName.scan(/coprocessor\$(\d+)/i)
                    maxId = ids[0][0].to_i if ids[0][0].to_i > maxId
                  end
                end
                maxId += 1
                htd.setValue(k + "\$" + maxId.to_s, value)
              end
            end
          end

          if arg[CONFIG]
            raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
            for k,v in arg[CONFIG]
              v = v.to_s unless v.nil?
              htd.setValue(k, v)
            end
          end
          @admin.modifyTable(table_name.to_java_bytes, htd)
          if wait == true
            puts "Updating all regions with the new schema..."
            alter_status(table_name)
          end
          next
        end

        # Unset table attributes
        if method == "table_att_unset"
          if arg.kind_of?(Hash)
            if (!arg[NAME])
              next
            end
            if (htd.getValue(arg[NAME]) == nil)
              raise ArgumentError, "Can not find attribute: #{arg[NAME]}"
            end
            htd.remove(arg[NAME].to_java_bytes)
            @admin.modifyTable(table_name.to_java_bytes, htd)
            if wait == true
              puts "Updating all regions with the new schema..."
              alter_status(table_name)
            end
          end
          next
        end

        # Unknown method
        raise ArgumentError, "Unknown method: #{method}"
      end
    end

    def status(format)
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
            puts("        %s" % [ region.getNameAsString() ])
            puts("            %s" % [ region.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
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

      raise(ArgumentError, "Column family #{arg} must have a name") unless name = arg[NAME]

      family = htd.getFamily(name.to_java_bytes)
      # create it if it's a new family
      family ||= org.apache.hadoop.hbase.HColumnDescriptor.new(name.to_java_bytes)

      family.setBlockCacheEnabled(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE)
      family.setScope(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE)
      family.setInMemory(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY)
      family.setTimeToLive(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::TTL])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::TTL)
      family.setDataBlockEncoding(org.apache.hadoop.hbase.io.encoding.DataBlockEncoding.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING)
      family.setEncodeOnDisk(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK)
      family.setBlocksize(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE)
      family.setMaxVersions(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS)
      family.setMinVersions(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS)
      family.setKeepDeletedCells(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS)
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER)
        bloomtype = arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER].upcase
        unless org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.include?(bloomtype)      
          raise(ArgumentError, "BloomFilter type #{bloomtype} is not supported. Use one of " + org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.join(" ")) 
        else 
          family.setBloomFilterType(org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.valueOf(bloomtype))
        end
      end
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION)
        compression = arg[org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION].upcase
        unless org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.constants.include?(compression)      
          raise(ArgumentError, "Compression #{compression} is not supported. Use one of " + org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.constants.join(" ")) 
        else 
          family.setCompressionType(org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.valueOf(compression))
        end
      end

      if arg[CONFIG]
        raise(ArgumentError, "#{CONFIG} must be a Hash type") unless arg.kind_of?(Hash)
        for k,v in arg[CONFIG]
          v = v.to_s unless v.nil?
          family.setValue(k, v)
        end
      end
      return family
    end

    #----------------------------------------------------------------------------------------------
    # Enables/disables a region by name
    def online(region_name, on_off)
      # Open meta table
      meta = org.apache.hadoop.hbase.client.HTable.new(org.apache.hadoop.hbase.HConstants::META_TABLE_NAME)

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

    #----------------------------------------------------------------------------------------------
    # Take a snapshot of specified table
    def snapshot(table, snapshot_name)
      @admin.snapshot(snapshot_name.to_java_bytes, table.to_java_bytes)
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
    # Returns a list of snapshots
    def list_snapshot
      @admin.listSnapshots
    end
  end
end
