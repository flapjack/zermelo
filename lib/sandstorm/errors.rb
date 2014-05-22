module Sandstorm

  module Errors

    class RecordNotFound
      attr_reader :klass, :id

      def initialize(k, i)
        @klass = k
        @id = i
      end
    end

    class RecordsNotFound
      attr_reader :klass, :ids

      def initialize(k, i)
        @klass = k
        @ids = i
      end
    end

  end

end