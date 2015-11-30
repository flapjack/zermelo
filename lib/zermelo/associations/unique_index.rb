# NB index instances are all internal to zermelo, not user-accessible

require 'zermelo/records/key'

module Zermelo
  module Associations
    class UniqueIndex

      def initialize(parent_klass, name)
        @parent_klass   = parent_klass
        @attribute_name = name

        @backend   = parent_klass.send(:backend)

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
        @backend.move(key, {@backend.index_keys(@attribute_type, value_from).join(':') => id},
          indexer_to.key, {@backend.index_keys(@attribute_type, value_to).join(':') => id})
      end

      def key
        @indexer ||= Zermelo::Records::Key.new(
          :klass  => @parent_klass,
          :name   => "by_#{@attribute_name}",
          :type   => :hash,
          :object => :index
        )
      end

      def key_dump
        [@backend.key_to_backend_key(key), key]
      end
    end
  end
end