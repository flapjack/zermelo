require 'forwardable'

require 'sandstorm'
require 'sandstorm/records/key'

module Sandstorm
  module Associations
    class HasMany

      extend Forwardable

      def_delegators :filter, :intersect, :union, :diff,
                       :count, :empty?, :exists?, :find_by_id,
                       :all, :each, :collect, :select, :find_all, :reject,
                       :ids

      def initialize(parent, name, options = {})
        @record_ids = Sandstorm::Records::Key.new(
          :class => parent.class.send(:class_key),
          :id    => parent.id,
          :name  => "#{name}_ids",
          :type  => :set
        )

        @name = name
        @parent = parent

        # TODO trap possible constantize error
        @associated_class = (options[:class_name] || name.classify).constantize

        @inverse = @associated_class.send(:inverse_of, name, @parent.class)
      end

      def <<(record)
        add(record)
        self  # for << 'a' << 'b'
      end

      def add(*records)
        raise 'No records to add' if records.empty?
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?}
        @parent.class.send(:lock, @parent.class, @associated_class) do
          unless @inverse.nil?
            records.each do |record|
              @associated_class.send(:load, record.id).send("#{@inverse}=", @parent)
            end
          end
          Sandstorm.redis.sadd(redis_key(@record_ids), records.map(&:id))
        end
      end

      # TODO support dependent delete, for now just deletes the association
      def delete(*records)
        raise 'No records to delete' if records.empty?
        raise 'Invalid record class' if records.any? {|r| !r.is_a?(@associated_class)}
        raise 'Record(s) must have been saved' unless records.all? {|r| r.persisted?}
        @parent.class.send(:lock, @parent.class, @associated_class) do
          unless @inverse.nil?
            records.each do |record|
              @associated_class.send(:load, record.id).send("#{@inverse}=", nil)
            end
          end
          Sandstorm.redis.srem(redis_key(@record_ids), records.map(&:id))
        end
      end

      private

      # TODO defined in backend, call there (or extract to key strategy)
      def redis_key(key)
        "#{key.klass}:#{key.id.nil? ? '' : key.id}:#{key.name}"
      end

      # associated will be a belongs_to; on remove already runs inside a lock
      def on_remove
        unless @inverse.nil?
          Sandstorm.redis.smembers(redis_key(@record_ids)).each do |record_id|
            @associated_class.send(:load, record_id).send("#{@inverse}=", nil)
          end
        end
        Sandstorm.redis.del(redis_key(@record_ids))
      end

      # TODO make generic to all backends
      def backend
        Sandstorm::Backends::RedisBackend.new
      end

      # creates a new filter class each time it's called, to store the
      # state for this particular filter chain
      def filter
        backend.filter(@record_ids, @associated_class)
      end

    end
  end
end