require 'sandstorm'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class BelongsTo

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:belongs_to", :hash)
        @parent = parent
        @name = name

        @inverse = options[:inverse_of] ? options[:inverse_of].to_s : nil

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
      end

      def value=(record)
        # TODO validate that record.is_a?(@associated_class)

        if @inverse
          record.send("#{@inverse}=".to_sym, @parent) if record.respond_to?("#{@inverse}=".to_sym)
        else
          Sandstorm.redis.hset(@record_ids.key, "#{@name}_id", record.id)
        end
      end

      def value
        return unless id = Sandstorm.redis.hget(@record_ids.key, "#{@name}_id")
        @associated_class.send(:load, id)
      end

    end
  end
end