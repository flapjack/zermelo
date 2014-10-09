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

      def delete_id(id, value)
        @backend.delete(key, @backend.index_keys(@attribute_type, value).join(':'))
      end

      def add_id(id, value)
        @backend.add(key, @backend.index_keys(@attribute_type, value).join(':') => id)
      end

      def move_id(id, value_from, indexer_to, value_to)
        @backend.move(key, {@backend.index_keys(@attribute_type, value_to).join(':') => id}, indexer_to.key)
      end

      def key
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