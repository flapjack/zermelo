require 'active_support/concern'

require 'sandstorm/locks/no_lock'

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

      def lock(*klasses, &block)
        ret = nil
        # doesn't handle re-entrant case for influxdb, which has no locking yet
        locking = Thread.current[:sandstorm_locking]
        if locking.nil?
          lock_proc = proc do
            begin
              Thread.current[:sandstorm_locking] = klasses
              ret = block.call
            ensure
              Thread.current[:sandstorm_locking] = nil
            end
          end

          lock_klass = case self
          when Sandstorm::Backends::RedisBackend
            Sandstorm::Locks::RedisLock
          else
            Sandstorm::Locks::NoLock
          end

          lock_klass.new.lock(*klasses, &lock_proc)
        else
          # accepts any subset of 'locking'
          unless (klasses - locking).empty?
            raise "Currently locking #{locking.map(&:name)}, cannot lock different set #{klasses.map(&:name)}"
          end
          ret = block.call
        end
        ret
      end

    end

  end

end