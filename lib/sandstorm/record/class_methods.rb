require 'forwardable'

require 'sandstorm/lock'

module Sandstorm

  module Record

    module ClassMethods

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :find_by_id, :find_by_ids, :find_by_id!, :find_by_ids!,
                       :all, :each, :collect,
                       :select, :find_all, :reject, :destroy_all,
                       :ids, :count, :empty?, :exists?

      def add_id(id)
        Sandstorm.redis.sadd(ids_key.key, id.to_s)
      end

      def delete_id(id)
        Sandstorm.redis.srem(ids_key.key, id.to_s)
      end

      def attribute_types
        ret = nil
        @lock.synchronize do
          ret = (@attribute_types ||= {}).dup
        end
        ret
      end

      protected

      def define_attributes(options = {})
        options.each_pair do |key, value|
          raise "Unknown attribute type ':#{value}' for ':#{key}'" unless
            Sandstorm::ALL_TYPES.include?(value)
          self.define_attribute_methods([key])
        end
        @lock.synchronize do
          (@attribute_types ||= {}).update(options)
        end
      end

      private

      def lock(*klasses)
        klasses |= [self]
        ret = nil
        locking = Thread.current[:sandstorm_locking]
        if locking.nil?
          Sandstorm::Lock.new(*klasses).lock do
            begin
              Thread.current[:sandstorm_locking] = klasses
              ret = yield
            ensure
              Thread.current[:sandstorm_locking] = nil
            end
          end
        else
          # accepts any subset of 'locking'
          unless (klasses - locking).empty?
            raise "Currently locking #{locking.map(&:name)}, cannot lock different set #{klasses.map(&:name)}"
          end
          ret = yield
        end
        ret
      end

      def ids_key
        @ids_key ||= Sandstorm::RedisKey.new("#{class_key}::ids", :set)
      end

      def class_key
        self.name.demodulize.underscore
      end

      def load(id)
        object = self.new
        object.load(id)
        object
      end

      def filter
        Sandstorm::Filter.new(ids_key, self)
      end

    end

  end

end
