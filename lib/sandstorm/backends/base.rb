require 'active_support/concern'

module Sandstorm

  module Backends

    module Base

      extend ActiveSupport::Concern

      def begin_transaction
        @steps = []
      end

      def add(key, value)
        # puts "add #{key.inspect} #{value.inspect}"
        @steps << [:add, key, value]
      end

      def delete(key, value)
        # puts "delete #{key.inspect} #{value.inspect}"
        @steps << [:delete, key, value]
      end

      def clear(key)
        # puts "clear #{key.inspect}"
        @steps << [:clear, key]
      end

      def commit_transaction
        # puts "commit txn"
        @steps = []
      end

      def abort_transaction
        # puts "abort txn"
        @steps = []
      end

    end

  end

end