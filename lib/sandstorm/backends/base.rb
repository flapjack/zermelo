require 'active_support/concern'

module Sandstorm

  module Backends

    module Base

      extend ActiveSupport::Concern

      # for hashes, lists, sets
      def add(key, value)
        change(:add, key, value)
      end

      def delete(key, value)
        change(:delete, key, value)
      end

      def move(key, value, key_to)
        change(:move, key, value, key_to)
      end

      def clear(key)
        change(:clear, key)
      end

      # works for both simple and complex types (i.e. strings, numbers, booleans,
      #  hashes, lists, sets)
      def set(key, value)
        change(:set, key, value)
      end

      def purge(key)
        change(:purge, key)
      end

      def get(attr_key)
        get_multiple(attr_key)[attr_key.klass][attr_key.id][attr_key.name.to_s]
      end

    end

  end

end