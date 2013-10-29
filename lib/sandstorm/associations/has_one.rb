require 'sandstorm'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class HasOne

      def initialize(parent, name, options = {})
        @record_id = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_id", :id)
        @parent = parent

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize

        @inverse = @associated_class.send(:inverse_of, name.to_sym)
      end

      def value
        return unless id = Sandstorm.redis.get(@record_id.key)
        @associated_class.send(:load, id)
      end

      def add(record)
        # TODO validate that record.is_a?(@associated_class)
        unless @inverse.nil?
          @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
        end
        Sandstorm.redis.set(@record_id.key, record.id)
      end

      def delete(record)
        # TODO validate that record.is_a?(@associated_class)
        unless @inverse.nil?
          record_id = Sandstorm.redis.get(@record_id.key)
          @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
        end
        Sandstorm.redis.del(@record_id.key)
      end

      private

      # associated may be a belongs_to
      def on_remove
        unless @inverse.nil?
          if record_id = Sandstorm.redis.get(@record_id.key)
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        Sandstorm.redis.del(@record_id.key)
      end

    end
  end
end