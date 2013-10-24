require 'forwardable'

require 'sandstorm'
require 'sandstorm/filter'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class HasMany

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :count, :empty?, :exists?, :find_by_id,
                       :all, :each, :collect, :select, :find_all, :reject,
                       :ids

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_ids", :set)
        @name = name
        @parent = parent
        @inverse = options[:inverse_of] ? options[:inverse_of].to_s : nil

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        # TODO collect all scores/ids and do a single zadd/single hmset
        records.each do |record|
          raise 'Invalid class' unless record.is_a?(@associated_class)
          # TODO next if already exists? in set
          record.save

          # TODO validate that record.is_a?(@associated_class)
          if @inverse
            inverse_id = Sandstorm::RedisKey.new("#{record.record_key}:belongs_to", :hash)
            Sandstorm.redis.hset(inverse_id.key, "#{@inverse}_id", @parent.id)
          end

          Sandstorm.redis.sadd(@record_ids.key, record.id)
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        Sandstorm.redis.srem(@record_ids.key, *records.map(&:id))
      end

      private

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        Sandstorm::Filter.new(@record_ids, @associated_class)
      end

    end
  end
end