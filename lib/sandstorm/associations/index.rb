require 'sandstorm/records/key'

module Sandstorm
  module Associations
    class Index

      def initialize(parent, class_key, att)
        @indexers = {}

        @parent = parent
        @class_key = class_key
        @attribute = att
      end

      def value=(value)
        @value = value
      end

      # TODO defined in backend, call there (or extract to key strategy)
      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      def delete_id(id)
        return unless indexer = indexer_for_value
        Sandstorm.redis.srem(redis_key(indexer), id)
      end

      def add_id(id)
        return unless indexer = indexer_for_value
        Sandstorm.redis.sadd(redis_key(indexer), id)
      end

      def move_id(id, indexer_to)
        return unless indexer = indexer_for_value
        Sandstorm.redis.smove(redis_key(indexer), redis_key(indexer_to), id)
      end

      def key
        return unless indexer = indexer_for_value
        redis_key(indexer)
      end

      private

      def indexer_for_value
        index_key = case @value
        when String, Symbol, TrueClass, FalseClass
          @value.to_s.gsub(/ /, '%20').gsub(/:/, '%3A')
        end
        return if index_key.nil?

        @indexers[index_key] ||= Sandstorm::Records::Key.new(
          :class => @class_key,
          :name  => "by_#{@attribute}:#{index_key}",
          :type  => :set
        )
      end

    end
  end
end