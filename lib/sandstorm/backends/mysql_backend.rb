require 'sandstorm/backends/base'

require 'sandstorm/filters/mysql_filter'

module Sandstorm

  module Backends

    class MySQLBackend

      include Sandstorm::Backends::Base

      def filter(ids_key, record)
        Sandstorm::Filters::MySQLFilter.new(self, ids_key, record)
      end

      def exists?(key)
      end

      def get_multiple(*attr_keys)
      end

      def begin_transaction
        return false if @in_transaction
        @in_transaction = true
        @changes = []
      end

      def commit_transaction
        return false unless @in_transaction
        apply_changes(@changes)
        @in_transaction = false
        @changes = []
      end

      def abort_transaction
        return false unless @in_transaction
        @in_transaction = false
        @changes = []
      end

      private

      def change(op, key, value = nil)
        ch = [op, key, value]
        if @in_transaction
          @changes << ch
        else
          apply_changes([ch])
        end
      end

      # composite all new changes into records, and then into influxdb
      # query statements
      def apply_changes(changes)
      end

    end

  end

end
