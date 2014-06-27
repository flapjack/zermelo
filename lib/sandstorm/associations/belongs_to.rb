require 'sandstorm'
require 'sandstorm/records/key'

# The other side of a has_one, has_many, or has_sorted_set association

module Sandstorm
  module Associations
    class BelongsTo

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::Records::Key.new(
          :class => parent.class.send(:class_key),
          :id    => parent.id,
          :name  => 'belongs_to',
          :type  => :hash
        )

        @parent = parent
        @name = name

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
        @class_name = options[:class_name].demodulize.underscore

        raise ':inverse_of must be set' if options[:inverse_of].nil?
        @inverse = options[:inverse_of].to_s
        @inverse_key = "#{@name}_id"
      end

      def inverse_of?(source)
        !source.nil? && (@inverse == source.to_s)
      end

      # intrinsically atomic, so no locking needed
      def value=(record)
        if record.nil?
          Sandstorm.redis.hdel(redis_key(@record_ids), @inverse_key)
        else
          raise 'Invalid record class' unless record.is_a?(@associated_class)
          raise 'Record must have been saved' unless record.persisted?
          Sandstorm.redis.hset(redis_key(@record_ids), @inverse_key, record.id)
        end
      end

      def value
        @parent.class.send(:lock, @parent.class, @associated_class) do
          if id = Sandstorm.redis.hget(redis_key(@record_ids), @inverse_key)
            @associated_class.send(:load, id)
          else
            nil
          end
        end
      end

      private

      # TODO defined in backend, call there (or extract to key strategy)
      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      # on remove already runs inside a lock
      def on_remove
        value.send("#{@inverse}_proxy".to_sym).send(:delete, @parent) unless value.nil?
        Sandstorm.redis.del(redis_key(@record_ids))
      end

    end
  end
end