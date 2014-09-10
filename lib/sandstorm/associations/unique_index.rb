# NB index instances are all internal to sandstorm, not user-accessible

module Sandstorm
  module Associations
    class UniqueIndex

      def initialize(parent, class_key, att)
        @indexers = {}

        @backend   = parent.send(:backend)
        @parent    = parent
        @class_key = class_key
        @attribute = att
      end

      def value
        @value
      end

      def value=(value)
        @value = value
      end

      def delete_id(id)
        @backend.delete(indexer, @value)
      end

      def add_id(id)
        @backend.add(indexer, @value => id)
      end

      def move_id(id, indexer_to)
        @backend.move(indexer, {indexer_to.value => id}, indexer_to.key)
      end

      def key
        indexer
      end

      # Raises RegexpError if the provided pattern is invalid
      def attributes_matching(pattern)
        regexp = Regexp.new(pattern)
        Sandstorm.redis.hkeys(indexer.key).select {|a| regexp === a }
      end

      private

      def indexer
        @indexer ||= Sandstorm::Records::Key.new(
          :class  => @class_key,
          :name   => "by_#{@attribute}",
          :type   => :hash,
          :object => :index
        )
      end

    end
  end
end