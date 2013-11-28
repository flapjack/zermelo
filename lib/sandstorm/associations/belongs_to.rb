require 'sandstorm'
require 'sandstorm/redis_key'

# The other side of a has_one, has_many, or has_sorted_set association

module Sandstorm
  module Associations
    class BelongsTo

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:belongs_to", :hash)
        @parent = parent
        @name = name

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
        @class_name = options[:class_name].demodulize.underscore

        raise ":inverse_of must be set" if options[:inverse_of].nil?
        @inverse = options[:inverse_of].to_s
        @inverse_key = "#{@name}_id"
      end

      def inverse_of?(source)
        !source.nil? && (@inverse == source.to_s)
      end

      def value=(record)
        if record.nil?
          Sandstorm.redis.hdel(@record_ids.key, @inverse_key)
        else
          # TODO validate that record.is_a?(@associated_class)
          raise "Record must have been saved" unless record.persisted?
          Sandstorm.redis.hset(@record_ids.key, @inverse_key, record.id)
        end
        # if Sandstorm.redis.hlen(@record_ids.key) == 0
        #   Sandstorm.redis.del(@record_ids.key)
        # end
      end

      def value
        return unless id = Sandstorm.redis.hget(@record_ids.key, @inverse_key)
        @associated_class.send(:load, id)
      end

      private

      def on_remove
        if record = value
          record.send("#{@inverse}_proxy".to_sym).send(:delete_without_lock, @parent)
        end
        Sandstorm.redis.del(@record_ids.key)
      end

    end
  end
end