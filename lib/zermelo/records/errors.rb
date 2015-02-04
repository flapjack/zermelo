module Zermelo

  class ZermeloError < RuntimeError
  end

  module Records
    module Errors
      class RecordNotFound < ::Zermelo::ZermeloError
        attr_reader :klass, :id

        def initialize(k, i)
          @klass = k
          @id = i
        end
      end

      class RecordsNotFound < ::Zermelo::ZermeloError
        attr_reader :klass, :ids

        def initialize(k, i)
          @klass = k
          @ids = i
        end
      end

      class RecordInvalid < ::Zermelo::ZermeloError
        attr_reader :record

        def initialize(r)
          @record = r
        end
      end

      class RecordNotSaved < ::Zermelo::ZermeloError
        attr_reader :record

        def initialize(r)
          @record = r
        end
      end
    end
  end
end