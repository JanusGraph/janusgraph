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

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class SecurityAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @config = configuration
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    def grant(user, permissions, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      # Verify that the specified permission is valid
      if (permissions == nil || permissions.length == 0)
        raise(ArgumentError, "Invalid permission: no actions associated with user")
      end

      if (table_name != nil)
        # Table should exist
        raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

        htd = @admin.getTableDescriptor(table_name.to_java_bytes)

        if (family != nil)
          raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
        end

        # invoke cp endpoint to perform access controlse
        fambytes = family.to_java_bytes if (family != nil)
        qualbytes = qualifier.to_java_bytes if (qualifier != nil)
        user_permission = org.apache.hadoop.hbase.security.access.UserPermission.new(
                                                user.to_java_bytes, table_name.to_java_bytes, 
                                                fambytes, qualbytes, permissions.to_java_bytes)
      else
        user_permission = org.apache.hadoop.hbase.security.access.UserPermission.new(
                                                user.to_java_bytes, permissions.to_java_bytes)
      end

      meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
                      org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
      protocol = meta_table.coprocessorProxy(
                      org.apache.hadoop.hbase.security.access.AccessControllerProtocol.java_class,
                                             org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
      begin
        protocol.grant(user_permission)
      rescue java.io.IOException => e
        if !(e.message.include? "java.lang.NoSuchMethodException")
          raise e
        end

        # Server has not the new API, try the old one
        if (table_name == nil)
          raise "Global permissions not supported by HBase Server"
        end

        tp = org.apache.hadoop.hbase.security.access.TablePermission.new(table_name.to_java_bytes, fambytes, qualbytes, permissions.to_java_bytes)
        protocol.grant(user.to_java_bytes, tp)
      end
    end

    #----------------------------------------------------------------------------------------------
    def revoke(user, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      if (table_name != nil)
        # Table should exist
        raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

        htd = @admin.getTableDescriptor(table_name.to_java_bytes)

        if (family != nil)
          raise(ArgumentError, "Can't find family: #{family}") unless htd.hasFamily(family.to_java_bytes)
        end

        # invoke cp endpoint to perform access control
        fambytes = family.to_java_bytes if (family != nil)
        qualbytes = qualifier.to_java_bytes if (qualifier != nil)
        user_permission = org.apache.hadoop.hbase.security.access.UserPermission.new(
                                                    user.to_java_bytes, table_name.to_java_bytes,
                                                    fambytes, qualbytes, "".to_java_bytes)
      else
        user_permission = org.apache.hadoop.hbase.security.access.UserPermission.new(
                                                    user.to_java_bytes, "".to_java_bytes)
      end

      meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
                        org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
      protocol = meta_table.coprocessorProxy(
                        org.apache.hadoop.hbase.security.access.AccessControllerProtocol.java_class,
                                             org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
      begin
        protocol.revoke(user_permission)
      rescue java.io.IOException => e
        if !(e.message.include? "java.lang.NoSuchMethodException")
          raise e
        end

        # Server has not the new API, try the old one
        if (table_name == nil)
          raise "Global permissions not supported by HBase Server"
        end

        tp = org.apache.hadoop.hbase.security.access.TablePermission.new(table_name.to_java_bytes, fambytes, qualbytes, "".to_java_bytes)
        protocol.revoke(user.to_java_bytes, tp)
      end
    end

    #----------------------------------------------------------------------------------------------
    def user_permission(table_name=nil)
      security_available?

      if (table_name != nil)
        raise(ArgumentError, "Can't find table: #{table_name}") unless exists?(table_name)
      end

      meta_table = org.apache.hadoop.hbase.client.HTable.new(@config, 
                        org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
      protocol = meta_table.coprocessorProxy(
                      org.apache.hadoop.hbase.security.access.AccessControllerProtocol.java_class,
                      org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
      perms = protocol.getUserPermissions(table_name != nil ? table_name.to_java_bytes : nil)

      res = {}
      count  = 0
      perms.each do |value|
        user_name = String.from_java_bytes(value.getUser)
        table = (value.getTable != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getTable) : ''
        family = (value.getFamily != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getFamily) : ''
        qualifier = (value.getQualifier != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getQualifier) : ''

        action = org.apache.hadoop.hbase.security.access.Permission.new value.getActions

        if block_given?
          yield(user_name, "#{table},#{family},#{qualifier}: #{action.to_s}")
        else
          res[user_name] ||= {}
          res[user_name][family + ":" +qualifier] = action
        end
        count += 1
      end
      
      return ((block_given?) ? count : res)
    end

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    # Make sure that security classes are available
    def security_available?()
      begin
        org.apache.hadoop.hbase.security.access.AccessControllerProtocol
      rescue NameError
        raise(ArgumentError, "DISABLED: Security features are not available in this build of HBase")
      end
    end

  end
end
