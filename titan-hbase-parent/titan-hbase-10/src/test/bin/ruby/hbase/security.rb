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

    def initialize(admin, formatter)
      @admin = admin
      @config = @admin.getConfiguration()
      @formatter = formatter
    end

    def close
      @admin.close
    end

    #----------------------------------------------------------------------------------------------
    def grant(user, permissions, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      begin
        # Verify that the specified permission is valid
        if (permissions == nil || permissions.length == 0)
          raise(ArgumentError, "Invalid permission: no actions associated with user")
        end

        perm = org.apache.hadoop.hbase.security.access.Permission.new(
                  permissions.to_java_bytes)

        if (table_name != nil)
          tablebytes=table_name.to_java_bytes
          #check if the tablename passed is actually a namespace
          if (isNamespace?(table_name))
            # Namespace should exist first.
            namespace_name = table_name[1...table_name.length]
            raise(ArgumentError, "Can't find a namespace: #{namespace_name}") unless
              namespace_exists?(namespace_name)

            org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
              @config, namespace_name, user, perm.getActions())
          else
            # Table should exist
            raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

            tableName = org.apache.hadoop.hbase.TableName.valueOf(table_name.to_java_bytes)
            htd = @admin.getTableDescriptor(tableName)

            if (family != nil)
             raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
            end

            fambytes = family.to_java_bytes if (family != nil)
            qualbytes = qualifier.to_java_bytes if (qualifier != nil)

            org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
              @config, tableName, user, fambytes, qualbytes, perm.getActions())
          end
        else
          # invoke cp endpoint to perform access controls
          org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
            @config, user, perm.getActions())
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    def revoke(user, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      begin
        if (table_name != nil)
          #check if the tablename passed is actually a namespace
          if (isNamespace?(table_name))
            # Namespace should exist first.
            namespace_name = table_name[1...table_name.length]
            raise(ArgumentError, "Can't find a namespace: #{namespace_name}") unless namespace_exists?(namespace_name)

            tablebytes=table_name.to_java_bytes
            org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
              @config, namespace_name, user)
          else
             # Table should exist
             raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

             tableName = org.apache.hadoop.hbase.TableName.valueOf(table_name.to_java_bytes)
             htd = @admin.getTableDescriptor(tableName)

             if (family != nil)
               raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
             end

             fambytes = family.to_java_bytes if (family != nil)
             qualbytes = qualifier.to_java_bytes if (qualifier != nil)

            org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
              @config, tableName, user, fambytes, qualbytes)
          end
        else
          perm = org.apache.hadoop.hbase.security.access.Permission.new(''.to_java_bytes)
          org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
            @config, user, perm.getActions())
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    def user_permission(table_regex=nil)
      security_available?
      all_perms = org.apache.hadoop.hbase.security.access.AccessControlClient.getUserPermissions(@config,table_regex)
      res = {}
      count  = 0
      all_perms.each do |value|
          user_name = String.from_java_bytes(value.getUser)
          if (table_regex != nil && isNamespace?(table_regex))
            namespace = table_regex[1...table_regex.length]
          else
            namespace = (value.getTableName != nil) ? value.getTableName.getNamespaceAsString() : ''
          end
          table = (value.getTableName != nil) ? value.getTableName.getNameAsString() : ''
          family = (value.getFamily != nil) ?
            org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getFamily) :
            ''
          qualifier = (value.getQualifier != nil) ?
            org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getQualifier) :
            ''

          action = org.apache.hadoop.hbase.security.access.Permission.new value.getActions

          if block_given?
            yield(user_name, "#{namespace},#{table},#{family},#{qualifier}: #{action.to_s}")
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

    def isNamespace?(table_name)
      table_name.start_with?('@')
    end

     # Does Namespace exist
    def namespace_exists?(namespace_name)
      namespaceDesc = @admin.getNamespaceDescriptor(namespace_name)
      if(namespaceDesc == nil)
        return false
      else
        return true
      end
    end

    # Make sure that security tables are available
    def security_available?()
      raise(ArgumentError, "DISABLED: Security features are not available") \
        unless exists?(org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
    end
  end
end
