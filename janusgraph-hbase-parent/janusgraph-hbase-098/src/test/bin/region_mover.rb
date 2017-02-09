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

# Moves regions. Will confirm region access in current location and will
# not move a new region until successful confirm of region loading in new
# location. Presumes balancer is disabled when we run (not harmful if its
# on but this script and balancer will end up fighting each other).
# Does not work for case of multiple regionservers all running on the
# one node.
require 'optparse'
include Java
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.conf.Configuration
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.HRegionInfo

# Name of this script
NAME = "region_mover"

# Get meta table reference
def getMetaTable(config)
  # Keep meta reference in ruby global
  if not $META
    $META = HTable.new(config, HConstants::META_TABLE_NAME)
  end
  return $META
end

# Get table instance.
# Maintains cache of table instances.
def getTable(config, name)
  # Keep dictionary of tables in ruby global
  if not $TABLES
    $TABLES = {}
  end
  key = name.toString()
  if not $TABLES[key]
    $TABLES[key] = HTable.new(config, name)
  end
  return $TABLES[key]
end

def closeTables()
  if not $TABLES
    return
  end

  $LOG.info("Close all tables")
  $TABLES.each do |name, table|
    $TABLES.delete(name)
    table.close()
  end
end

# Returns true if passed region is still on 'original' when we look at .META.
def isSameServer(admin, r, original)
  server = getServerNameForRegion(admin, r)
  return false unless server
  return true unless original
  return server == original
end

class RubyAbortable
  include org.apache.hadoop.hbase.Abortable
  def abort(why, e)
    puts "ABORTED! why=" + why + ", e=" + e.to_s
  end
end

# Get servername that is up in .META.; this is hostname + port + startcode comma-delimited.
# Can return nil
def getServerNameForRegion(admin, r)
  if r.isMetaRegion()
    # Hack
    zkw = org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.new(admin.getConfiguration(), "region_mover", nil)
    begin
      tracker = org.apache.hadoop.hbase.zookeeper.MetaRegionTracker.new(zkw, RubyAbortable.new())
      tracker.start()
      while not tracker.isLocationAvailable()
        sleep 0.1
      end
      # Make a fake servername by appending ','
      metaServer = tracker.getMetaRegionLocation().toString() + ","
      tracker.stop()
      return metaServer
    ensure
      zkw.close()
    end
  end
  table = nil
  table = getMetaTable(admin.getConfiguration())
  g = Get.new(r.getRegionName())
  g.addColumn(HConstants::CATALOG_FAMILY, HConstants::SERVER_QUALIFIER)
  g.addColumn(HConstants::CATALOG_FAMILY, HConstants::STARTCODE_QUALIFIER)
  result = table.get(g)
  server = result.getValue(HConstants::CATALOG_FAMILY, HConstants::SERVER_QUALIFIER)
  startcode = result.getValue(HConstants::CATALOG_FAMILY, HConstants::STARTCODE_QUALIFIER)
  return nil unless server
  return java.lang.String.new(Bytes.toString(server)).replaceFirst(":", ",")  + "," + Bytes.toLong(startcode).to_s
end

# Trys to scan a row from passed region
# Throws exception if can't
def isSuccessfulScan(admin, r)
  scan = Scan.new(r.getStartKey()) 
  scan.setBatch(1)
  scan.setCaching(1)
  scan.setFilter(FirstKeyOnlyFilter.new()) 
  table = getTable(admin.getConfiguration(), r.getTableName()) 
  scanner = table.getScanner(scan)
  begin
    results = scanner.next() 
    # We might scan into next region, this might be an empty table.
    # But if no exception, presume scanning is working.
  ensure
    scanner.close()
    # Do not close the htable. It is cached in $TABLES and 
    # may be reused in moving another region of same table. 
    # table.close()
  end
end

# Check region has moved successful and is indeed hosted on another server
# Wait until that is the case.
def move(admin, r, newServer, original)
  # Now move it. Do it in a loop so can retry if fail.  Have seen issue where
  # we tried move region but failed and retry put it back on old location;
  # retry in this case.
  retries = admin.getConfiguration.getInt("hbase.move.retries.max", 5)
  count = 0
  same = true
  start = Time.now
  while count < retries and same
    if count > 0
      $LOG.info("Retry " + count.to_s + " of maximum " + retries.to_s)
    end
    count = count + 1
    begin
      admin.move(Bytes.toBytes(r.getEncodedName()), Bytes.toBytes(newServer))
    rescue java.lang.reflect.UndeclaredThrowableException => e
      $LOG.info("Exception moving "  + r.getEncodedName() +
        "; split/moved? Continuing: " + e)
      return
    end
    # Wait till its up on new server before moving on
    maxWaitInSeconds = admin.getConfiguration.getInt("hbase.move.wait.max", 60)
    maxWait = Time.now + maxWaitInSeconds
    while Time.now < maxWait
      same = isSameServer(admin, r, original)
      break unless same
      sleep 0.1
    end
  end
  raise RuntimeError, "Region stuck on #{original}, newserver=#{newServer}" if same
  # Assert can Scan from new location.
  isSuccessfulScan(admin, r)
  $LOG.info("Moved region " + r.getRegionNameAsString() + " cost: " + 
    java.lang.String.format("%.3f", (Time.now - start)))
end

# Return the hostname portion of a servername (all up to first ',')
def getHostnamePortFromServerName(serverName)
  parts = serverName.split(',')
  return parts[0] + ":" + parts[1]
end

# Return the hostname:port out of a servername (all up to first ',')
def getHostnameFromServerName(serverName)
  return serverName.split(',')[0]
end

# Return array of servernames where servername is hostname+port+startcode
# comma-delimited
def getServers(admin)
  serverInfos = admin.getClusterStatus().getServerInfo()
  servers = []
  for server in serverInfos
    servers << server.getServerName()
  end
  return servers
end

# Remove the servername whose hostname portion matches from the passed
# array of servers.  Returns as side-effect the servername removed.
def stripServer(servers, hostname)
  count = servers.length
  servername = nil
  for server in servers
    if getHostnameFromServerName(server) == hostname
      servername = servers.delete(server)
    end
  end
  # Check server to exclude is actually present
  raise RuntimeError, "Server %s not online" % hostname unless servers.length < count
  return servername
end

# Returns a new serverlist that excludes the servername whose hostname portion
# matches from the passed array of servers.
def stripExcludes(servers, excludefile)
  excludes = readExcludes(excludefile)
  servers =  servers.find_all{|server| !excludes.contains(getHostnameFromServerName(server)) }
  # return updated servers list
  return servers
end


# Return servername that matches passed hostname
def getServerName(servers, hostname)
  servername = nil
  for server in servers
    if getHostnameFromServerName(server) == hostname
      servername = server
      break
    end
  end
  raise ArgumentError, "Server %s not online" % hostname unless servername
  return servername
end

# Create a logger and disable the DEBUG-level annoying client logging
def configureLogging(options)
  apacheLogger = LogFactory.getLog(NAME)
  # Configure log4j to not spew so much
  unless (options[:debug]) 
    logger = org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase.client")
    logger.setLevel(org.apache.log4j.Level::INFO)
  end
  return apacheLogger
end

# Get configuration instance
def getConfiguration()
  config = HBaseConfiguration.create()
  # No prefetching on .META.
  config.setInt("hbase.client.prefetch.limit", 1)
  # Make a config that retries at short intervals many times
  config.setInt("hbase.client.pause", 500)
  config.setInt("hbase.client.retries.number", 100)
  return config
end

# Now get list of regions on targetServer
def getRegions(config, servername)
  connection = HConnectionManager::getConnection(config);
  return ProtobufUtil::getOnlineRegions(connection.getAdmin(ServerName.new(servername)));
end

def deleteFile(filename)
  f = java.io.File.new(filename)
  f.delete() if f.exists()
end

# Write HRegionInfo to file
# Need to serialize in case non-printable characters.
# Format is count of regionnames followed by serialized regionnames.
def writeFile(filename, regions)
  fos = java.io.FileOutputStream.new(filename)
  dos = java.io.DataOutputStream.new(fos)
  # Write out a count of region names
  dos.writeInt(regions.size())
  # Write actual region names.
  for r in regions
    Bytes.writeByteArray(dos, r.toByteArray())
  end
  dos.close()
end

# See writeFile above.
# Returns array of HRegionInfos
def readFile(filename)
  f = java.io.File.new(filename)
  return java.util.ArrayList.new() unless f.exists()
  fis = java.io.FileInputStream.new(f)
  dis = java.io.DataInputStream.new(fis)
  # Read count of regions
  count = dis.readInt()
  regions = java.util.ArrayList.new(count)
  index = 0
  while index < count
    regions.add(HRegionInfo.parseFromOrNull(Bytes.readByteArray(dis)))
    index = index + 1
  end
  dis.close()
  return regions
end

# Move regions off the passed hostname
def unloadRegions(options, hostname)
  # Get configuration
  config = getConfiguration()
  # Clean up any old files.
  filename = getFilename(options, hostname)
  deleteFile(filename)
  # Get an admin instance
  admin = HBaseAdmin.new(config) 
  servers = getServers(admin)
  # Remove the server we are unloading from from list of servers.
  # Side-effect is the servername that matches this hostname 
  servername = stripServer(servers, hostname)

  # Remove the servers in our exclude list from list of servers.
  servers = stripExcludes(servers, options[:excludesFile])
  puts "Valid region move targets: ", servers
  movedRegions = java.util.ArrayList.new()
  while true
    rs = getRegions(config, servername)
    break if rs.length == 0
    count = 0
    $LOG.info("Moving " + rs.length.to_s + " region(s) from " + servername +
      " during this cycle");
    for r in rs
      # Get a random server to move the region to.
      server = servers[rand(servers.length)]
      $LOG.info("Moving region " + r.getRegionNameAsString() + " (" + 
        count.to_s + " of " + rs.length.to_s + ") from server=" + 
        servername + " to server=" + server);
      count = count + 1
      # Assert we can scan region in its current location
      isSuccessfulScan(admin, r)
      # Now move it.
      move(admin, r, server, servername)
      movedRegions.add(r)
    end
  end
  if movedRegions.size() > 0 
    # Write out file of regions moved
    writeFile(filename, movedRegions)
    $LOG.info("Wrote list of moved regions to " + filename)
  end
end

# Move regions to the passed hostname
def loadRegions(options, hostname)
  # Get configuration
  config = getConfiguration()
  # Get an admin instance
  admin = HBaseAdmin.new(config) 
  filename = getFilename(options, hostname) 
  regions = readFile(filename)
  return if regions.isEmpty()
  servername = nil
  # Wait till server is up
  maxWaitInSeconds = admin.getConfiguration.getInt("hbase.serverstart.wait.max", 180)
  maxWait = Time.now + maxWaitInSeconds
  while Time.now < maxWait
    servers = getServers(admin)
    begin
      servername = getServerName(servers, hostname)
    rescue ArgumentError => e
      $LOG.info("hostname=" + hostname.to_s + " is not up yet, waiting");
    end
    break if servername
    sleep 0.5
  end
  $LOG.info("Moving " + regions.size().to_s + " regions to " + servername)
  count = 0
  # sleep 20s to make sure the rs finished initialization.
  sleep 20
  for r in regions
    exists = false
    begin
      isSuccessfulScan(admin, r)
      exists = true
    rescue org.apache.hadoop.hbase.NotServingRegionException => e
      $LOG.info("Failed scan of " + e.message)
    end
    count = count + 1
    next unless exists
    currentServer = getServerNameForRegion(admin, r)
    if currentServer and currentServer == servername
      $LOG.info("Region " + r.getRegionNameAsString() + " (" + count.to_s +
        " of " + regions.length.to_s + ") already on target server=" + servername) 
      next
    end
    $LOG.info("Moving region " + r.getRegionNameAsString() + " (" + 
      count.to_s + " of " + regions.length.to_s + ") from server=" + 
      currentServer + " to server=" + servername);
    move(admin, r, servername, currentServer)
  end
end

# Returns an array of hosts to exclude as region move targets
def readExcludes(filename)
  if filename == nil
    return java.util.ArrayList.new()
  end 
  if ! File.exist?(filename)  
      puts "Error: Unable to read host exclude file: ", filename
      raise RuntimeError
  end 

  f = File.new(filename, "r")
  # Read excluded hosts list
  excludes = java.util.ArrayList.new()
  while (line = f.gets)
    line.strip! # do an inplace drop of pre and post whitespaces
    excludes.add(line) unless line.empty? # exclude empty lines
  end
  puts "Excluding hosts as region move targets: ", excludes
  f.close
  
  return excludes
end

def getFilename(options, targetServer)
  filename = options[:file]
  if not filename
    filename = "/tmp/" + ENV['USER'] + targetServer
  end
  return filename
end


# Do command-line parsing
options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: #{NAME}.rb [options] load|unload <hostname>"
  opts.separator 'Load or unload regions by moving one at a time'
  options[:file] = nil
  opts.on('-f', '--filename=FILE', 'File to save regions list into unloading, or read from loading; default /tmp/<hostname>') do |file|
    options[:file] = file
  end
  opts.on('-h', '--help', 'Display usage information') do
    puts opts
    exit
  end
  options[:debug] = false
  opts.on('-d', '--debug', 'Display extra debug logging') do
    options[:debug] = true
  end
  opts.on('-x', '--excludefile=FILE', 'File with hosts-per-line to exclude as unload targets; default excludes only target host; useful for rack decommisioning.') do |file|
    options[:excludesFile] = file
  end
end
optparse.parse!

# Check ARGVs
if ARGV.length < 2
  puts optparse
  exit 1
end
hostname = ARGV[1]
if not hostname
  opts optparse
  exit 2
end
# Create a logger and save it to ruby global
$LOG = configureLogging(options) 
case ARGV[0]
  when 'load'
    loadRegions(options, hostname)
  when 'unload'
    unloadRegions(options, hostname)
  else
    puts optparse
    exit 3
end

closeTables()
