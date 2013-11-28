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
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.lock(@parent.class, @associated_class) do
          unless @inverse.nil?
            records.each do |record|
              @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
            end
          end
          Sandstorm.redis.zadd(@record_ids.key, *(records.map {|r| [r.send(@key.to_sym).to_f, r.id]}.flatten))
        end
      end

      def delete(*records)
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.lock(@parent.class, @associated_class) do
          delete_without_lock(*records)
        end
      end

      private

      def delete_without_lock(*records)
        unless @inverse.nil?
          records.each do |record|
            @associated_class.send(:load, record.id).send("#{@inverse}=", nil)
          end
        end
        Sandstorm.redis.zrem(@record_ids.key, *records.map(&:id))
      end

      # associated will be a belongs_to; on remove already runs inside a lock
      def on_remove
        unless @inverse.nil?
          Sandstorm.redis.zrange(@record_ids.key, 0, -1).each do |record_id|
            # clear the belongs_to inverse value with this @parent.id

            # TODO -- replace all @associated_class.send(:load, record_id).send
            # with direct code for what the other side will do -- if this leads
            # to code duplication, move said code to a mixin but don't load
            # it directly

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