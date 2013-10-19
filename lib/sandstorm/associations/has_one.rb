require 'sandstorm'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class HasOne

      def initialize(parent, name, options = {})
        @record_id = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_id", :id)
        @parent = parent
        @inverse = options[:inverse_of] ? options[:inverse_of].to_s : nil

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
      end

      def value=(record)
        # TODO validate that record.is_a?(@associated_class)

        if @inverse
          inverse_id = Sandstorm::RedisKey.new("#{record.record_key}:belongs_to", :hash)
          Sandstorm.redis.hset(inverse_id.key, "#{@inverse}_id", @parent.id)
        end

        Sandstorm.redis.set(@record_id.key, record.id)
      end

      def value
        return unless id = Sandstorm.redis.get(@record_id.key)
        @associated_class.send(:load, id)
      end

    end
  end
end