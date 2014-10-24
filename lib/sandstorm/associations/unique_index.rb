# NB index instances are all internal to sandstorm, not user-accessible

module Sandstorm
  module Associations
    class UniqueIndex

      def initialize(parent_klass, name)
        @parent_klass   = parent_klass
        @attribute_name = name

        @backend   = parent_klass.send(:backend)
        @class_key = parent_klass.send(:class_key)

        @indexers = {}

        parent_klass.send(:with_index_data, name.to_sym) do |data|
          @attribute_type = data.type
        end
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