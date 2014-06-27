require 'sandstorm/records/key'

module Sandstorm
  module Associations
    class UniqueIndex

      def initialize(parent, class_key, att)
        @indexers = {}

        @parent = parent
        @class_key = class_key
        @attribute = att
      end

      def value
        @value
      end

      def value=(value)
        @value = value
      end

      def delete_id(id)
        Sandstorm.redis.hdel(redis_key(indexer), @value)
      end

      def add_id(id)
        Sandstorm.redis.hset(redis_key(indexer), @value, id)
      end

      def move_id(id, indexer_to)
        # TODO locking
        Sandstorm.redis.hdel(redis_key(indexer), @value)
        Sandstorm.redis.hset(redis_key(indexer), indexer_to.value, id)
      end

      def key
        redis_key(indexer)
      end

      private

      # TODO defined in backend, call there (or extract to key strategy)
      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      def indexer
        @indexer ||= Sandstorm::Records::Key.new(
          :class => @class_key,
          :name  => "by_#{@attribute}",
          :type  => :hash
        )
      end

    end
  end
end