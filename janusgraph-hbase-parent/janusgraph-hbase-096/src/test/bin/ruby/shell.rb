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

# Shell commands module
module Shell
  @@commands = {}
  def self.commands
    @@commands
  end

  @@command_groups = {}
  def self.command_groups
    @@command_groups
  end

  def self.load_command(name, group)
    return if commands[name]

    # Register command in the group
    raise ArgumentError, "Unknown group: #{group}" unless command_groups[group]
    command_groups[group][:commands] << name

    # Load command
    begin
      require "shell/commands/#{name}"
      klass_name = name.to_s.gsub(/(?:^|_)(.)/) { $1.upcase } # camelize
      commands[name] = eval("Commands::#{klass_name}")
    rescue => e
      raise "Can't load hbase shell command: #{name}. Error: #{e}\n#{e.backtrace.join("\n")}"
    end
  end

  def self.load_command_group(group, opts)
    raise ArgumentError, "No :commands for group #{group}" unless opts[:commands]

    command_groups[group] = {
      :commands => [],
      :command_names => opts[:commands],
      :full_name => opts[:full_name] || group,
      :comment => opts[:comment]
    }

    opts[:commands].each do |command|
      load_command(command, group)
    end
  end

  #----------------------------------------------------------------------
  class Shell
    attr_accessor :hbase
    attr_accessor :formatter

    @debug = false
    attr_accessor :debug

    def initialize(hbase, formatter)
      self.hbase = hbase
      self.formatter = formatter
    end

    def hbase_admin
      @hbase_admin ||= hbase.admin(formatter)
    end

    def hbase_table(name)
      hbase.table(name, self)
    end

    def hbase_replication_admin
      @hbase_replication_admin ||= hbase.replication_admin(formatter)
    end

    def hbase_security_admin
      @hbase_security_admin ||= hbase.security_admin(formatter)
    end

    def export_commands(where)
      ::Shell.commands.keys.each do |cmd|
        # here where is the IRB namespace
        # this method just adds the call to the specified command
        # which just references back to 'this' shell object
        # a decently extensible way to add commands
        where.send :instance_eval, <<-EOF
          def #{cmd}(*args)
            ret = @shell.command('#{cmd}', *args)
            puts
            return ret
          end
        EOF
      end
    end

    def command_instance(command)
      ::Shell.commands[command.to_s].new(self)
    end

    #call the method 'command' on the specified command
    def command(command, *args)
      internal_command(command, :command, *args)
    end

    #call a specific internal method in the command instance
    # command  - name of the command to call
    # method_name - name of the method on the command to call. Defaults to just 'command'
    # args - to be passed to the named method
    def internal_command(command, method_name= :command, *args)
      command_instance(command).command_safe(self.debug,method_name, *args)
    end

    def print_banner
      puts "HBase Shell; enter 'help<RETURN>' for list of supported commands."
      puts 'Type "exit<RETURN>" to leave the HBase Shell'
      print 'Version '
      command('version')
      puts
    end

    def help_multi_command(command)
      puts "Command: #{command}"
      puts command_instance(command).help
      puts
      return nil
    end

    def help_command(command)
      puts command_instance(command).help
      return nil
    end

    def help_group(group_name)
      group = ::Shell.command_groups[group_name.to_s]
      group[:commands].sort.each { |cmd| help_multi_command(cmd) }
      if group[:comment]
        puts '-' * 80
        puts
        puts group[:comment]
        puts
      end
      return nil
    end

    def help(command = nil)
      if command
        return help_command(command) if ::Shell.commands[command.to_s]
        return help_group(command) if ::Shell.command_groups[command.to_s]
        puts "ERROR: Invalid command or command group name: #{command}"
        puts
      end

      puts help_header
      puts
      puts 'COMMAND GROUPS:'
      ::Shell.command_groups.each do |name, group|
        puts "  Group name: " + name
        puts "  Commands: " + group[:command_names].sort.join(', ')
        puts
      end
      unless command
        puts 'SHELL USAGE:'
        help_footer
      end
      return nil
    end

    def help_header
      return "HBase Shell, version #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()}, " +
             "r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()}, " +
             "#{org.apache.hadoop.hbase.util.VersionInfo.getDate()}" + "\n" +
        "Type 'help \"COMMAND\"', (e.g. 'help \"get\"' -- the quotes are necessary) for help on a specific command.\n" +
        "Commands are grouped. Type 'help \"COMMAND_GROUP\"', (e.g. 'help \"general\"') for help on a command group."
    end

    def help_footer
      puts <<-HERE
Quote all names in HBase Shell such as table and column names.  Commas delimit
command parameters.  Type <RETURN> after entering a command to run it.
Dictionaries of configuration used in the creation and alteration of tables are
Ruby Hashes. They look like this:

  {'key1' => 'value1', 'key2' => 'value2', ...}

and are opened and closed with curley-braces.  Key/values are delimited by the
'=>' character combination.  Usually keys are predefined constants such as
NAME, VERSIONS, COMPRESSION, etc.  Constants do not need to be quoted.  Type
'Object.constants' to see a (messy) list of all constants in the environment.

If you are using binary keys or values and need to enter them in the shell, use
double-quote'd hexadecimal representation. For example:

  hbase> get 't1', "key\\x03\\x3f\\xcd"
  hbase> get 't1', "key\\003\\023\\011"
  hbase> put 't1', "test\\xef\\xff", 'f1:', "\\x01\\x33\\x40"

The HBase shell is the (J)Ruby IRB with the above HBase-specific commands added.
For more on the HBase Shell, see http://hbase.apache.org/docs/current/book.html
      HERE
    end
  end
end

# Load commands base class
require 'shell/commands'

# Load all commands
Shell.load_command_group(
  'general',
  :full_name => 'GENERAL HBASE SHELL COMMANDS',
  :commands => %w[
    status
    version
    table_help
    whoami
  ]
)

Shell.load_command_group(
  'ddl',
  :full_name => 'TABLES MANAGEMENT COMMANDS',
  :commands => %w[
    alter
    create
    describe
    disable
    disable_all
    is_disabled
    drop
    drop_all
    enable
    enable_all
    is_enabled
    exists
    list
    show_filters
    alter_status
    alter_async
    get_table
  ]
)

Shell.load_command_group(
  'namespace',
  :full_name => 'NAMESPACE MANAGEMENT COMMANDS',
  :commands => %w[
    create_namespace
    drop_namespace
    alter_namespace
    describe_namespace
    list_namespace
    list_namespace_tables
  ]
)

Shell.load_command_group(
  'dml',
  :full_name => 'DATA MANIPULATION COMMANDS',
  :commands => %w[
    count
    delete
    deleteall
    get
    get_counter
    incr
    put
    scan
    truncate
    truncate_preserve
  ]
)

Shell.load_command_group(
  'tools',
  :full_name => 'HBASE SURGERY TOOLS',
  :comment => "WARNING: Above commands are for 'experts'-only as misuse can damage an install",
  :commands => %w[
    assign
    balancer
    balance_switch
    close_region
    compact
    flush
    major_compact
    move
    split
    merge_region
    unassign
    zk_dump
    hlog_roll
    catalogjanitor_run
    catalogjanitor_switch
    catalogjanitor_enabled
    trace
  ]
)

Shell.load_command_group(
  'replication',
  :full_name => 'CLUSTER REPLICATION TOOLS',
  :comment => "In order to use these tools, hbase.replication must be true.",
  :commands => %w[
    add_peer
    remove_peer
    list_peers
    enable_peer
    disable_peer
    list_replicated_tables
  ]
)

Shell.load_command_group(
  'snapshot',
  :full_name => 'CLUSTER SNAPSHOT TOOLS',
  :commands => %w[
    snapshot
    clone_snapshot
    restore_snapshot
    rename_snapshot
    delete_snapshot
    list_snapshots
  ]
)

Shell.load_command_group(
  'security',
  :full_name => 'SECURITY TOOLS',
  :comment => "NOTE: Above commands are only applicable if running with the AccessController coprocessor",
  :commands => %w[
    grant
    revoke
    user_permission
  ]
)

