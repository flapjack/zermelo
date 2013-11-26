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

        @associated_class = (options[:class_name] || name.classify).constantize
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_ids", :sorted_set)

        @inverse = @associated_class.send(:inverse_of, name.to_sym, @parent.class)
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      # TODO collect all scores/ids and do a single zadd/single hmset
      def add(*records)
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        records.each do |record|
          raise 'Invalid class' unless record.is_a?(@associated_class)
          record.save
          unless @inverse.nil?
            @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
          end
          Sandstorm.redis.zadd(@record_ids.key, record.send(@key.to_sym).to_f, record.id)
        end
      end

      def delete(*records)
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        unless @inverse.nil?
          records.each do |record|
            @associated_class.send(:load, record.id).send("#{@inverse}=", nil)
          end
        end
        Sandstorm.redis.zrem(@record_ids.key, records.map(&:id))
        if Sandstorm.redis.zcard(@record_ids.key) == 0
          Sandstorm.redis.del(@record_ids.key)
        end
      end

      private

      def on_remove
        unless @inverse.nil?
          Sandstorm.redis.zrange(@record_ids.key, 0, -1).each do |record_id|
            # clear the belongs_to inverse value with this @parent.id
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        Sandstorm.redis.del(@record_ids.key)
      end

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        Sandstorm::Filter.new(@record_ids, @associated_class)
      end

    end
  end
end