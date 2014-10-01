# NB index instances are all internal to sandstorm, not user-accessible

module Sandstorm
  module Associations
    class Index

      def initialize(parent, class_key, att_name, att_type)
        @indexers = {}

        @backend   = parent.send(:backend)
        @parent    = parent
        @class_key = class_key
        @attribute_name = att_name
        @attribute_type = att_type
      end

      def value=(value)
        @value = value
      end

      def delete_id(id)
        return unless indexer = indexer_for_value

        @backend.delete(indexer, id)
      end

      def add_id(id)
        return unless indexer = indexer_for_value

        @backend.add(indexer, id)
      end

      def move_id(id, indexer_to)
        return unless indexer = indexer_for_value

        @backend.move(indexer, id, indexer_to.key)
      end

      def key
        indexer_for_value
      end

      private

      def indexer_for_value
        index_keys = @backend.index_keys(@attribute_type, @value)
        raise "Can't index '#{@value}' (#{@attribute_type}" if index_keys.nil?

        @indexers[index_keys.join(":")] ||= Sandstorm::Records::Key.new(
          :class  => @class_key,
          :name   => "by_#{@attribute_name}:#{index_keys.join(':')}",
          :type   => :set,
          :object => :index
        )
      end

    end
  end
end