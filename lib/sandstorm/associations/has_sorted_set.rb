require 'sandstorm'
require 'sandstorm/filter'
require 'sandstorm/redis_key'

module Sandstorm
  module Associations
    class HasSortedSet

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

      def intersect(opts = {})
        new_filter.intersect(opts)
      end

      def union(opts = {})
        new_filter.union(opts)
      end

      def intersect_range(start, finish, options = {})
        new_filter.intersect_range(options.merge(:start => start, :end => finish,
                                                 :by_score => options[:by_score]))
      end

      def union_range(start, finish, options = {})
        new_filter.union_range(options.merge(:start => start, :end => finish,
                                             :by_score => options[:by_score]))
      end

      def count
        new_filter.count
      end

      def empty?
        new_filter.empty?
      end

      def all
        new_filter.all
      end

      def first
        new_filter.first
      end

      def last
        new_filter.last
      end

      def collect(&block)
        new_filter.collect(&block)
      end

      def each(&block)
        new_filter.each(&block)
      end

      def ids
        new_filter.ids
      end

      private

      def new_filter
        Sandstorm::Filter.new(@record_ids, @associated_class)
      end

    end
  end
end