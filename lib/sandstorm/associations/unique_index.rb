# NB index instances are all internal to sandstorm, not user-accessible

module Sandstorm
  module Associations
    class UniqueIndex

      def initialize(parent, class_key, att_name, att_type)
        @indexers = {}

        @backend   = parent.send(:backend)
        @parent    = parent
        @class_key = class_key
        @attribute_name = att_name
        @attribute_type = att_type
      end

      def value
        @value
      end

      def value=(value)
        @value = value
      end

      def delete_id(id)
        @backend.delete(indexer, @backend.index_keys(@attribute_type, value).join(':'))
      end

      def add_id(id)
        @backend.add(indexer, @backend.index_keys(@attribute_type, @value).join(':') => id)
      end

      def move_id(id, indexer_to)
        @backend.move(indexer, {@backend.index_keys(@attribute_type, indexer_to.value).join(':') => id}, indexer_to.key)
      end

      def key
        indexer
      end

      private

      def indexer
        @indexer ||= Sandstorm::Records::Key.new(
          :class  => @class_key,
          :name   => "by_#{@attribute_name}",
          :type   => :hash,
          :object => :index
        )
      end

    end
  end
end