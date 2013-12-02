require 'forwardable'

require 'sandstorm'
require 'sandstorm/filter'
require 'sandstorm/redis_key'

# much like a has_many, but with different add/remove behaviour, as it's paired
# with another has_and_belongs_to_many association. both sides must set the
# inverse association name.

module Sandstorm
  module Associations
    class HasAndBelongsToMany

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :count, :empty?, :exists?, :find_by_id,
                       :all, :each, :collect, :select, :find_all, :reject,
                       :ids

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::RedisKey.new("#{parent.record_key}:#{name}_ids", :set)
        @name = name
        @parent = parent

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize

        raise ":inverse_of must be set" if options[:inverse_of].nil?
        @inverse = options[:inverse_of].to_s
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        raise 'Invalid record class' unless records.all? {|r| r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.send(:lock, @parent.class, @associated_class) do
          records.each do |record|
            @associated_class.send(:load, record.id).send(@inverse.to_sym).
              send(:add_without_inverse, @parent)
          end
          add_without_inverse(*records)
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        raise 'Invalid record class' unless records.all? {|r| r.is_a?(@associated_class)}
        raise "Record(s) must have been saved" unless records.all? {|r| r.persisted?}
        @parent.class.send(:lock, @parent.class, @associated_class) do
          records.each do |record|
            @associated_class.send(:load, record.id).send(@inverse.to_sym).
              send(:delete_without_inverse, @parent)
          end
          delete_without_inverse(*records)
        end
      end

      private

      def add_without_inverse(*records)
        Sandstorm.redis.sadd(@record_ids.key, records.map(&:id))
      end

      def delete_without_inverse(*records)
        Sandstorm.redis.srem(@record_ids.key, records.map(&:id))
      end

      # associated will be the other side of the HaBTM; on_remove is always
      # called inside a lock
      def on_remove
        Sandstorm.redis.smembers(@record_ids.key).each do |record_id|
          @associated_class.send(:load, record_id).send(@inverse.to_sym).
            send(:delete_without_inverse, @parent)
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