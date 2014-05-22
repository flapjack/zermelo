require 'sandstorm/redis_key'

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

      def delete_id(id)
        return unless indexer = indexer_for_value
        Sandstorm.redis.srem(indexer.key, id)
      end

      def add_id(id)
        return unless indexer = indexer_for_value
        Sandstorm.redis.sadd(indexer.key, id)
      end

      def move_id(id, indexer_to)
        return unless indexer = indexer_for_value
        Sandstorm.redis.smove(indexer.key, indexer_to.key, id)
      end

      def key
        return unless indexer = indexer_for_value
        indexer.key
      end

      # Raises RegexpError if the provided pattern is invalid
      # (will likely have been invoked with .send("#{att}_index", nil) as
      #  value is not needed)
      def attributes_matching(pattern)
        regexp = Regexp.new(pattern)
        Sandstorm.redis.keys("#{@class_key}::by_#{@attribute}:*").inject([]) do |memo, k|
          if k =~ /^#{@class_key}::by_#{@attribute}:(.+)$/
            att = $1.gsub(/%3A/, ':').gsub(/%20/, ' ').gsub(/%%/, '%')
            memo << att if (regexp === att)
          end
          memo
        end
      end

      private

      def indexer_for_value
        index_key = case @value
        when String, Symbol, TrueClass, FalseClass
          @value.to_s.gsub(/%/, '%%').gsub(/ /, '%20').gsub(/:/, '%3A')
        end
        return if index_key.nil?

        @indexers[index_key] ||= Sandstorm::RedisKey.new("#{@class_key}::by_#{@attribute}:#{index_key}", :set)
      end

    end
  end
end