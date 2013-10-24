require 'sandstorm'
require 'sandstorm/filter'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class HasSortedSet

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :intersect_range, :union_range,
                       :count, :empty?, :exists?, :find_by_id,
                       :all, :each, :collect, :select, :find_all, :reject,
                       :first, :last, :ids

      def initialize(parent, name, options = {})
        @key = options[:key]
        @parent = parent
        @name = name
        @inverse = options[:inverse_of] ? options[:inverse_of].to_s : nil
        @associated_class = (options[:class_name] || name.classify).constantize
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_ids", :sorted_set)
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        # TODO collect all scores/ids and do a single zadd/single hmset
        records.each do |record|
          raise 'Invalid class' unless record.is_a?(@associated_class)
          record.save

          # TODO validate that record.is_a?(@associated_class)
          if @inverse
            inverse_id = Sandstorm::RedisKey.new("#{record.record_key}:belongs_to", :hash)
            Sandstorm.redis.hset(inverse_id.key, "#{@inverse}_id", @parent.id)
          end

          Sandstorm.redis.zadd(@record_ids.key, record.send(@key.to_sym).to_f, record.id)
        end
      end

      def delete(*records)
        Sandstorm.redis.zrem(@record_ids.key, records.map(&:id))
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