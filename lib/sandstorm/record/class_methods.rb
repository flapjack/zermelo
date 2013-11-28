
module Sandstorm

  module Record

    module ClassMethods

      def count
        Sandstorm.redis.scard(ids_key)
      end

      def ids
        Sandstorm.redis.smembers(ids_key)
      end

      def add_id(id)
        Sandstorm.redis.sadd(ids_key, id.to_s)
      end

      def delete_id(id)
        Sandstorm.redis.srem(ids_key, id.to_s)
      end

      def exists?(id)
        Sandstorm.redis.sismember(ids_key, id.to_s)
      end

      def all
        lock { ids.collect {|id| load(id) } }
      end

      def delete_all
        lock do
          ids.each do |id|
            next unless record = load(id)
            record.destroy
          end
        end
      end

      def intersect(opts = {})
        Sandstorm::Filter.new(Sandstorm::RedisKey.new(ids_key, :set), self).intersect(opts)
      end

      def union(opts = {})
        Sandstorm::Filter.new(Sandstorm::RedisKey.new(ids_key, :set), self).union(opts)
      end

      def diff(opts = {})
        Sandstorm::Filter.new(Sandstorm::RedisKey.new(ids_key, :set), self).diff(opts)
      end

      def find_by_id(id)
        lock do
          if id && exists?(id.to_s)
            load(id.to_s)
          else
            nil
          end
        end
      end

      def attribute_types
        ret = nil
        @lock.synchronize do
          ret = (@attribute_types ||= {}).dup
        end
        ret
      end

      def lock(*klasses)
        klasses = [self] if klasses.empty?
        klass_lock = Sandstorm::Lock.new(*klasses)
        ret = nil
        klass_lock.lock { ret = yield }
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

      def ids_key
        "#{class_key}::ids"
      end

      def class_key
        self.name.demodulize.underscore
      end

      def load(id)
        object = self.new
        object.load(id)
        object
      end

    end

  end

end
