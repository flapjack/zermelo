module Sandstorm
  module Records
    module Errors

      class RecordNotFound < RuntimeError
        attr_reader :klass, :id

        def initialize(k, i)
          @klass = k
          @id = i
        end
      end

      class RecordsNotFound < RuntimeError
        attr_reader :klass, :ids

        def initialize(k, i)
          @klass = k
          @ids = i
        end
      end
   end
 end
end